package loadgen

import "io"

// WriteGatewayRecordingJSON 输出已收尾报告，不创建或关闭 writer，错误报告也可保存。
// 校验和编码先于写入；短写及原始写入错误保留，输出以换行结束。
func WriteGatewayRecordingJSON(w io.Writer, report GatewayRecordingReport) error {
	return writeRecordingJSON(w, recordingReport[GatewaySamplingConfig](report), samplingConfigJSON{Endpoint: report.Config.Endpoint, IntervalNS: int64(report.Config.Interval), RequestTimeoutNS: int64(report.Config.RequestTimeout)}, "")
}
