import { execFileSync } from "node:child_process";
import https from "node:https";

const endpoint = "https://manly-preacher-scallop.ngrok-free.dev/mcp-server/http";
const endpointUrl = new URL(endpoint);

function readToken() {
  if (process.env.N8N_MCP_ACCESS_TOKEN) return process.env.N8N_MCP_ACCESS_TOKEN;

  if (process.platform === "win32") {
    try {
      return execFileSync(
        "powershell.exe",
        [
          "-NoProfile",
          "-Command",
          "[Environment]::GetEnvironmentVariable('N8N_MCP_ACCESS_TOKEN', 'User')",
        ],
        { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
      ).trim();
    } catch {
      return "";
    }
  }

  return "";
}

const token = readToken();
if (!token) {
  console.error("N8N_MCP_ACCESS_TOKEN is not set.");
  process.exit(1);
}

const debugEnabled = process.env.N8N_MCP_BRIDGE_DEBUG === "1";

function debug(message) {
  if (debugEnabled) console.error(`[n8n-mcp-bridge] ${message}`);
}

let input = Buffer.alloc(0);
let outputMode = "frame";

process.stdin.on("data", (chunk) => {
  debug(`stdin chunk bytes=${chunk.length}`);
  input = Buffer.concat([input, chunk]);
  void drainInput();
});

async function drainInput() {
  while (true) {
    const firstNonWhitespace = input.toString("utf8", 0, Math.min(input.length, 16)).search(/\S/);
    if (firstNonWhitespace !== -1 && input[firstNonWhitespace] === 123) {
      const lineEnd = input.indexOf("\n");
      if (lineEnd === -1) return;

      outputMode = "ndjson";
      const line = input.subarray(0, lineEnd).toString("utf8").trim();
      input = input.subarray(lineEnd + 1);
      if (!line) continue;

      try {
        const message = JSON.parse(line);
        debug(`received method=${message.method ?? "response"} id=${message.id ?? "none"}`);
        void forward(message);
      } catch (error) {
        console.error(`Invalid MCP message: ${error.message}`);
      }
      continue;
    }

    let separatorLength = 4;
    let headerEnd = input.indexOf("\r\n\r\n");
    if (headerEnd === -1) {
      headerEnd = input.indexOf("\n\n");
      separatorLength = 2;
    }
    if (headerEnd === -1) {
      debug(`waiting for header separator input=${JSON.stringify(input.toString("latin1").slice(0, 200))}`);
      return;
    }

    const header = input.subarray(0, headerEnd).toString("utf8");
    const lengthMatch = header.match(/content-length:\s*(\d+)/i);
    if (!lengthMatch) {
      input = input.subarray(headerEnd + separatorLength);
      continue;
    }

    const length = Number(lengthMatch[1]);
    const bodyStart = headerEnd + separatorLength;
    const bodyEnd = bodyStart + length;
    if (input.length < bodyEnd) return;

    const body = input.subarray(bodyStart, bodyEnd).toString("utf8");
    input = input.subarray(bodyEnd);

    try {
      const message = JSON.parse(body);
      debug(`received method=${message.method ?? "response"} id=${message.id ?? "none"}`);
      void forward(message);
    } catch (error) {
      console.error(`Invalid MCP message: ${error.message}`);
    }
  }
}

async function forward(message) {
  const hasId = Object.hasOwn(message, "id");

  try {
    debug(`forwarding method=${message.method ?? "response"} hasId=${hasId}`);
    const response = await postMessage(message);

    debug(`http status=${response.status} bytes=${response.text.length}`);
    if (response.status < 200 || response.status >= 300) {
      throw new Error(`HTTP ${response.status}`);
    }

    if (!hasId) return;

    const payloads = parseResponse(response.text, response.contentType);
    const payload = payloads.find((item) => item.id === message.id) ?? payloads[0];
    debug(`parsed payloads=${payloads.length} willWrite=${Boolean(payload)}`);
    if (payload) writeFrame(payload);
  } catch (error) {
    if (hasId) {
      writeFrame({
        jsonrpc: "2.0",
        id: message.id,
        error: {
          code: -32000,
          message: error.message,
        },
      });
    } else {
      console.error(`MCP notification failed: ${error.message}`);
    }
  }
}

function postMessage(message) {
  const body = Buffer.from(JSON.stringify(message), "utf8");

  return new Promise((resolve, reject) => {
    const request = https.request(
      endpointUrl,
      {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${token}`,
          "Content-Type": "application/json",
          "Content-Length": String(body.length),
          "Accept": "application/json, text/event-stream",
          "ngrok-skip-browser-warning": "true",
        },
      },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => {
          resolve({
            status: response.statusCode ?? 0,
            contentType: response.headers["content-type"] ?? "",
            text: Buffer.concat(chunks).toString("utf8"),
          });
        });
      },
    );

    request.on("error", reject);
    request.end(body);
  });
}

function parseResponse(text, contentType) {
  if (contentType.includes("text/event-stream")) {
    const payloads = [];
    let data = [];

    for (const line of text.split(/\r?\n/)) {
      if (line.startsWith("data:")) {
        data.push(line.slice(5).trimStart());
      } else if (line === "" && data.length > 0) {
        pushJson(payloads, data.join("\n"));
        data = [];
      }
    }

    if (data.length > 0) pushJson(payloads, data.join("\n"));
    return payloads;
  }

  const parsed = JSON.parse(text);
  return Array.isArray(parsed) ? parsed : [parsed];
}

function pushJson(payloads, value) {
  try {
    payloads.push(JSON.parse(value));
  } catch {
    // Ignore non-JSON SSE keepalive messages.
  }
}

function writeFrame(message) {
  if (outputMode === "ndjson") {
    process.stdout.write(`${JSON.stringify(message)}\n`);
    return;
  }

  const body = Buffer.from(JSON.stringify(message), "utf8");
  process.stdout.write(`Content-Length: ${body.length}\r\n\r\n`);
  process.stdout.write(body);
}
