const ui = {
  token: document.querySelector("#token"),
  language: document.querySelector("#language"),
  start: document.querySelector("#start"),
  pause: document.querySelector("#pause"),
  stop: document.querySelector("#stop"),
  status: document.querySelector("#status"),
  epoch: document.querySelector("#epoch"),
  offset: document.querySelector("#offset"),
  finals: document.querySelector("#finals"),
  partial: document.querySelector("#partial"),
  error: document.querySelector("#error"),
};

let socket;
let streamInfo;
let resumeToken;
let audioContext;
let mediaStream;
let worklet;
let paused = false;
let ending = false;
let sampleOffset = 0n;
let reconnectStarted = 0;

function setStatus(text, state = "idle") {
  ui.status.textContent = text;
  ui.status.dataset.state = state;
}

function showError(message) {
  ui.error.textContent = message;
  if (message) setStatus("发生错误", "error");
}

async function accessToken() {
  if (ui.token.value.trim()) return ui.token.value.trim();
  const response = await fetch("/dev/token", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({tenant_id: "demo-clinic", subject: "demo-doctor"}),
  });
  if (!response.ok) throw new Error("请提供有效的 access token");
  const body = await response.json();
  ui.token.value = body.access_token;
  return body.access_token;
}

async function createStream() {
  const response = await fetch("/v1/streams", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${await accessToken()}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({language_code: ui.language.value.trim()}),
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error?.message || `创建会话失败 (${response.status})`);
  }
  streamInfo = await response.json();
  resumeToken = streamInfo.attach_token;
  sessionStorage.setItem(`tide:${streamInfo.stream_id}`, resumeToken);
}

function connect() {
  return new Promise((resolve, reject) => {
    socket = new WebSocket(streamInfo.websocket_url);
    socket.binaryType = "arraybuffer";
    socket.onopen = () => socket.send(JSON.stringify({type: "hello", token: resumeToken}));
    socket.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type === "ready") {
        resumeToken = message.resume_token;
        sessionStorage.setItem(`tide:${streamInfo.stream_id}`, resumeToken);
        sampleOffset = BigInt(message.next_sample_offset || 0);
        ui.offset.textContent = sampleOffset.toString();
        ui.epoch.textContent = message.epoch;
        reconnectStarted = 0;
        ui.error.textContent = "";
        setStatus(paused ? "已暂停" : "实时收音", "live");
        resolve();
      } else if (message.type === "transcript") {
        if (message.is_final) {
          const paragraph = document.createElement("p");
          paragraph.textContent = message.text;
          ui.finals.appendChild(paragraph);
          ui.partial.textContent = "";
        } else {
          ui.partial.textContent = message.text;
        }
      } else if (message.type === "ack") {
        ui.offset.textContent = String(message.next_sample_offset);
      } else if (message.type === "discontinuity") {
        ui.epoch.textContent = message.epoch;
        showError("服务节点已切换，当前 epoch 的 ASR 上下文已重新建立。");
      } else if (message.type === "error") {
        showError(`${message.code}: ${message.message}`);
      } else if (message.type === "ended") {
        teardown(false);
        setStatus("已结束");
      }
    };
    socket.onerror = () => reject(new Error("WebSocket 连接失败"));
    socket.onclose = () => {
      if (!ending && streamInfo && resumeToken) scheduleReconnect();
    };
  });
}

function scheduleReconnect() {
  if (ending || !streamInfo || !resumeToken) return;
  if (!reconnectStarted) reconnectStarted = Date.now();
  if (Date.now() - reconnectStarted > 30000) {
    showError("断线超过 30 秒，会话无法续接。");
    teardown(false);
    return;
  }
  setStatus("正在重连…");
  setTimeout(() => {
    if (!ending && streamInfo) connect().catch(scheduleReconnect);
  }, 1000);
}

async function startAudio() {
  mediaStream = await navigator.mediaDevices.getUserMedia({
    audio: {channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true},
  });
  audioContext = new AudioContext();
  await audioContext.audioWorklet.addModule("/pcm-worklet.js");
  const source = audioContext.createMediaStreamSource(mediaStream);
  worklet = new AudioWorkletNode(audioContext, "tide-pcm16-resampler");
  worklet.port.onmessage = ({data}) => {
    if (paused || !socket || socket.readyState !== WebSocket.OPEN) return;
    const samples = new Float32Array(data);
    const frame = new ArrayBuffer(8 + samples.length * 2);
    const view = new DataView(frame);
    view.setBigUint64(0, sampleOffset);
    for (let i = 0; i < samples.length; i++) {
      const value = Math.max(-1, Math.min(1, samples[i]));
      view.setInt16(8 + i * 2, value < 0 ? value * 32768 : value * 32767, true);
    }
    socket.send(frame);
    sampleOffset += BigInt(samples.length);
  };
  source.connect(worklet);
  worklet.connect(audioContext.destination);
}

async function begin() {
  try {
    showError("");
    ending = false;
    ui.start.disabled = true;
    setStatus("正在创建会话…");
    await createStream();
    await connect();
    await startAudio();
    ui.pause.disabled = false;
    ui.stop.disabled = false;
  } catch (error) {
    showError(error.message);
    teardown(false);
  }
}

function togglePause() {
  paused = !paused;
  ui.pause.textContent = paused ? "继续收音" : "暂停收音";
  setStatus(paused ? "已暂停" : "实时收音", paused ? "idle" : "live");
}

function finish() {
  ending = true;
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({type: "end"}));
  setTimeout(() => teardown(true), 500);
}

function teardown(closeSocket = true) {
  ending = true;
  if (closeSocket) socket?.close();
  mediaStream?.getTracks().forEach(track => track.stop());
  audioContext?.close();
  socket = undefined;
  mediaStream = undefined;
  audioContext = undefined;
  streamInfo = undefined;
  paused = false;
  ui.start.disabled = false;
  ui.pause.disabled = true;
  ui.pause.textContent = "暂停收音";
  ui.stop.disabled = true;
}

ui.start.addEventListener("click", begin);
ui.pause.addEventListener("click", togglePause);
ui.stop.addEventListener("click", finish);
