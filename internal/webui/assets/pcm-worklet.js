class PCM16Resampler extends AudioWorkletProcessor {
  constructor() {
    super();
    this.pending = [];
    this.pendingLength = 0;
    this.inputPerFrame = sampleRate / 16000 * 640;
  }

  process(inputs) {
    const channel = inputs[0] && inputs[0][0];
    if (!channel || channel.length === 0) return true;
    this.pending.push(new Float32Array(channel));
    this.pendingLength += channel.length;
    if (this.pendingLength < Math.ceil(this.inputPerFrame)) return true;

    const joined = new Float32Array(this.pendingLength);
    let cursor = 0;
    for (const block of this.pending) {
      joined.set(block, cursor);
      cursor += block.length;
    }

    let consumed = 0;
    while (joined.length - consumed >= Math.ceil(this.inputPerFrame)) {
      const output = new Float32Array(640);
      const ratio = sampleRate / 16000;
      for (let i = 0; i < output.length; i++) {
        output[i] = joined[consumed + Math.floor(i * ratio)] || 0;
      }
      this.port.postMessage(output, [output.buffer]);
      consumed += Math.floor(this.inputPerFrame);
    }
    const remainder = joined.slice(consumed);
    this.pending = remainder.length ? [remainder] : [];
    this.pendingLength = remainder.length;
    return true;
  }
}

registerProcessor("tide-pcm16-resampler", PCM16Resampler);
