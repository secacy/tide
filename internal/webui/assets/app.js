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

const MAX_RECONNECT_MS = 30_000;
const MAX_BUFFERED_SAMPLES = 16_000 * 30;
const MAX_SOCKET_BUFFER_BYTES = 256 * 1024;

let socket;
let streamInfo;
let currentAccessToken;
let resumeToken;
let resumeTokenExpiresAt = 0;
let audioContext;
let mediaStream;
let worklet;
let paused = false;
let ending = false;
let connectionReady = false;
let hasConnected = false;
let currentEpoch = 0;

let captureOffset = 0n;
let committedOffset = 0n;
let nextSendOffset = 0n;
let bufferedSamples = 0;
let audioFrames = [];

let reconnectStarted = 0;
let reconnectAttempts = 0;
let reconnectTimer;
let connectInFlight;
let flushTimer;

const segmentVersions = new Map();
const partialSegments = new Map();
const finalElements = new Map();

function setStatus(text, state = "idle") {
  ui.status.textContent = text;
  ui.status.dataset.state = state;
}

function showError(message) {
  ui.error.textContent = message;
  if (message) setStatus("发生错误", "error");
}

function resetSessionState() {
  captureOffset = 0n;
  committedOffset = 0n;
  nextSendOffset = 0n;
  bufferedSamples = 0;
  audioFrames = [];
  currentEpoch = 0;
  hasConnected = false;
  connectionReady = false;
  reconnectStarted = 0;
  reconnectAttempts = 0;
  segmentVersions.clear();
  partialSegments.clear();
  finalElements.clear();
  ui.finals.replaceChildren();
  ui.partial.textContent = "等待音频…";
  ui.epoch.textContent = "—";
  ui.offset.textContent = "0";
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
  currentAccessToken = await accessToken();
  const response = await fetch("/v1/streams", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${currentAccessToken}`,
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
  resumeTokenExpiresAt = Date.parse(streamInfo.expires_at) || 0;
  saveResumeToken();
}

function saveResumeToken() {
  if (streamInfo && resumeToken) {
    sessionStorage.setItem(`tide:${streamInfo.stream_id}`, resumeToken);
  }
}

async function rotateResumeToken() {
  const response = await fetch(`/v1/streams/${streamInfo.stream_id}/resume-token`, {
    method: "POST",
    headers: {"Authorization": `Bearer ${currentAccessToken}`},
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(body.error?.message || `获取重连凭证失败 (${response.status})`);
    error.terminal = response.status === 401 || response.status === 403 || response.status === 404 || response.status === 410;
    throw error;
  }
  resumeToken = body.resume_token;
  resumeTokenExpiresAt = Date.parse(body.expires_at) || 0;
  saveResumeToken();
}

function connectOnce() {
  if (connectInFlight) return connectInFlight;
  const attempt = new Promise((resolve, reject) => {
    const candidate = new WebSocket(streamInfo.websocket_url);
    candidate.binaryType = "arraybuffer";
    socket = candidate;
    connectionReady = false;
    let settled = false;
    let deliveredReady = false;
    const timeout = setTimeout(() => candidate.close(4000, "ready timeout"), 5000);

    const rejectOnce = error => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      reject(error);
    };

    candidate.onopen = () => candidate.send(JSON.stringify({type: "hello", token: resumeToken}));
    candidate.onmessage = event => {
      let message;
      try {
        message = JSON.parse(event.data);
      } catch {
        candidate.close(1002, "invalid JSON event");
        return;
      }
      if (message.type === "ready") {
        try {
          handleReady(message);
        } catch (error) {
          failSession(error.message);
          rejectOnce(error);
          return;
        }
        deliveredReady = true;
        if (!settled) {
          settled = true;
          clearTimeout(timeout);
          resolve();
        }
      } else {
        handleServerEvent(message);
      }
    };
    candidate.onerror = () => {};
    candidate.onclose = event => {
      clearTimeout(timeout);
      if (socket === candidate) {
        socket = undefined;
        connectionReady = false;
      }
      if (!deliveredReady) {
        const error = new Error(`WebSocket 连接失败 (${event.code})`);
        error.closeCode = event.code;
        rejectOnce(error);
      } else if (!ending && streamInfo) {
        setTimeout(scheduleReconnect, 0);
      }
    };
  });
  connectInFlight = attempt;
  attempt.finally(() => {
    if (connectInFlight === attempt) connectInFlight = undefined;
  }).catch(() => {});
  return attempt;
}

function handleReady(message) {
  const serverOffset = BigInt(message.next_sample_offset || 0);
  const epoch = Number(message.epoch || 0);
  if (!hasConnected) {
    captureOffset = serverOffset;
    committedOffset = serverOffset;
    nextSendOffset = serverOffset;
    hasConnected = true;
  } else {
    const sameEpoch = epoch === currentEpoch;
    const resumeOffset = sameEpoch && committedOffset > serverOffset ? committedOffset : serverOffset;
    const oldestOffset = audioFrames.length ? audioFrames[0].offset : captureOffset;
    if (resumeOffset < oldestOffset || resumeOffset > captureOffset) {
      throw new Error(`音频恢复点不在本地缓存中 (${resumeOffset})`);
    }
    nextSendOffset = resumeOffset;
  }
  currentEpoch = epoch;
  resumeToken = message.resume_token;
  resumeTokenExpiresAt = Date.parse(message.expires_at) || 0;
  saveResumeToken();
  ui.offset.textContent = committedOffset.toString();
  ui.epoch.textContent = String(currentEpoch);
  reconnectStarted = 0;
  reconnectAttempts = 0;
  connectionReady = true;
  ui.error.textContent = "";
  setStatus(paused ? "已暂停" : "实时收音", "live");
  flushAudio();
}

function handleServerEvent(message) {
  switch (message.type) {
  case "transcript":
    handleTranscript(message);
    break;
  case "ack":
    handleAck(BigInt(message.next_sample_offset || 0));
    break;
  case "discontinuity":
    partialSegments.clear();
    renderPartials();
    ui.epoch.textContent = String(message.epoch);
    ui.error.textContent = "服务节点已切换，ASR 上下文存在断层。";
    setStatus("已恢复（存在断层）", "live");
    break;
  case "error":
    if (message.code === "connection_replaced") {
      failSession("当前录音连接已被另一个页面替换。");
    } else {
      showError(`${message.code}: ${message.message}`);
    }
    break;
  case "ended":
    teardown(false);
    setStatus("已结束");
    break;
  }
}

function handleTranscript(message) {
  const key = `${message.epoch || currentEpoch}:${message.segment_id}`;
  const revision = Number(message.revision || 0);
  const previous = segmentVersions.get(key);
  if (previous && (previous.isFinal || revision < previous.revision ||
      (revision === previous.revision && !message.is_final))) return;
  segmentVersions.set(key, {revision, isFinal: Boolean(message.is_final)});
  if (message.is_final) {
    partialSegments.delete(key);
    let paragraph = finalElements.get(key);
    if (!paragraph) {
      paragraph = document.createElement("p");
      finalElements.set(key, paragraph);
      ui.finals.appendChild(paragraph);
    }
    paragraph.textContent = message.text;
  } else {
    partialSegments.set(key, message.text);
  }
  renderPartials();
}

function renderPartials() {
  ui.partial.textContent = partialSegments.size ? [...partialSegments.values()].join("\n") : "";
}

function handleAck(offset) {
  if (offset <= committedOffset) return;
  if (offset > captureOffset) {
    failSession(`服务端 ACK 超出本地采样位置 (${offset})`);
    return;
  }
  committedOffset = offset;
  while (audioFrames.length && audioFrames[0].end <= committedOffset) {
    const frame = audioFrames.shift();
    bufferedSamples -= frame.samples;
  }
  ui.offset.textContent = committedOffset.toString();
  flushAudio();
}

function enqueueAudio(samples) {
  if (bufferedSamples + samples.length > MAX_BUFFERED_SAMPLES) {
    failSession("断线音频缓存已满，录音已停止以避免静默丢失。", true);
    return;
  }
  const offset = captureOffset;
  const data = new ArrayBuffer(8 + samples.length * 2);
  const view = new DataView(data);
  view.setBigUint64(0, offset);
  for (let i = 0; i < samples.length; i++) {
    const value = Math.max(-1, Math.min(1, samples[i]));
    view.setInt16(8 + i * 2, value < 0 ? value * 32768 : value * 32767, true);
  }
  const end = offset + BigInt(samples.length);
  audioFrames.push({offset, end, samples: samples.length, data});
  bufferedSamples += samples.length;
  captureOffset = end;
  flushAudio();
}

function flushAudio() {
  clearTimeout(flushTimer);
  flushTimer = undefined;
  if (!connectionReady || !socket || socket.readyState !== WebSocket.OPEN || ending) return;
  while (nextSendOffset < captureOffset && socket.bufferedAmount < MAX_SOCKET_BUFFER_BYTES) {
    const frame = audioFrames.find(item => item.offset <= nextSendOffset && nextSendOffset < item.end);
    if (!frame) {
      failSession(`待发送音频存在缺口 (${nextSendOffset})`);
      return;
    }
    let wire = frame.data;
    if (nextSendOffset > frame.offset) {
      const skippedSamples = Number(nextSendOffset - frame.offset);
      const pcm = new Uint8Array(frame.data, 8 + skippedSamples * 2);
      wire = new ArrayBuffer(8 + pcm.byteLength);
      const view = new DataView(wire);
      view.setBigUint64(0, nextSendOffset);
      new Uint8Array(wire, 8).set(pcm);
    }
    socket.send(wire);
    nextSendOffset = frame.end;
  }
  if (nextSendOffset < captureOffset) {
    flushTimer = setTimeout(flushAudio, 25);
  }
}

function scheduleReconnect() {
  if (ending || !streamInfo || reconnectTimer || connectInFlight) return;
  if (!reconnectStarted) reconnectStarted = Date.now();
  if (Date.now() - reconnectStarted >= MAX_RECONNECT_MS) {
    failSession("断线超过 30 秒，会话无法续接。");
    return;
  }
  setStatus("正在重连…");
  const base = Math.min(200 * (2 ** reconnectAttempts), 2000);
  const delay = Math.round(base * (0.75 + Math.random() * 0.5));
  reconnectAttempts++;
  reconnectTimer = setTimeout(runReconnect, delay);
}

async function runReconnect() {
  reconnectTimer = undefined;
  if (ending || !streamInfo) return;
  try {
    if (resumeTokenExpiresAt && Date.now() >= resumeTokenExpiresAt - 1000) {
      await rotateResumeToken();
    }
    await connectOnce();
  } catch (error) {
    if (error.closeCode === 4401 || error.closeCode === 4409) {
      try {
        await rotateResumeToken();
      } catch (rotateError) {
        if (rotateError.terminal) {
          failSession(rotateError.message);
          return;
        }
      }
    } else if (error.terminal) {
      failSession(error.message);
      return;
    }
    scheduleReconnect();
  }
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
    if (!paused && !ending && streamInfo) enqueueAudio(new Float32Array(data));
  };
  source.connect(worklet);
  worklet.connect(audioContext.destination);
}

async function begin() {
  try {
    showError("");
    ending = false;
    resetSessionState();
    ui.start.disabled = true;
    setStatus("正在创建会话…");
    await createStream();
    await connectOnce();
    await startAudio();
    ui.pause.disabled = false;
    ui.stop.disabled = false;
  } catch (error) {
    teardown(true);
    showError(error.message);
  }
}

async function togglePause() {
  paused = !paused;
  if (paused) await audioContext?.suspend();
  else await audioContext?.resume();
  ui.pause.textContent = paused ? "继续收音" : "暂停收音";
  setStatus(paused ? "已暂停" : "实时收音", paused ? "idle" : "live");
}

function finish() {
  ending = true;
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({type: "end"}));
  setTimeout(() => teardown(true), 500);
}

function failSession(message, sendEnd = false) {
  if (ending) return;
  if (sendEnd && socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({type: "end"}));
  }
  teardown(true);
  showError(message);
}

function teardown(closeSocket = true) {
  ending = true;
  clearTimeout(reconnectTimer);
  clearTimeout(flushTimer);
  reconnectTimer = undefined;
  flushTimer = undefined;
  connectionReady = false;
  connectInFlight = undefined;
  const currentSocket = socket;
  socket = undefined;
  if (closeSocket) currentSocket?.close();
  mediaStream?.getTracks().forEach(track => track.stop());
  audioContext?.close();
  if (streamInfo) sessionStorage.removeItem(`tide:${streamInfo.stream_id}`);
  mediaStream = undefined;
  audioContext = undefined;
  worklet = undefined;
  streamInfo = undefined;
  resumeToken = undefined;
  resumeTokenExpiresAt = 0;
  currentAccessToken = undefined;
  paused = false;
  audioFrames = [];
  bufferedSamples = 0;
  ui.start.disabled = false;
  ui.pause.disabled = true;
  ui.pause.textContent = "暂停收音";
  ui.stop.disabled = true;
}

ui.start.addEventListener("click", begin);
ui.pause.addEventListener("click", togglePause);
ui.stop.addEventListener("click", finish);
