package defaultimpl

import "IM2/internal/common"

// contentPreviewOf 根据消息类型返回会话列表展示用的内容预览字符串。
// 文本类消息直接返回原始文本；多媒体类消息返回固定占位符。
func contentPreviewOf(msgType int32, content string) string {
	switch common.MessageType(msgType) {
	case common.MessageType_CHAT_TEXT, common.MessageType_GROUP_TEXT:
		return content
	case common.MessageType_CHAT_IMAGE, common.MessageType_GROUP_IMAGE:
		return "[图片]"
	case common.MessageType_CHAT_VIDEO, common.MessageType_GROUP_VIDEO:
		return "[视频]"
	case common.MessageType_CHAT_AUDIO, common.MessageType_GROUP_AUDIO:
		return "[语音]"
	case common.MessageType_CHAT_FILE, common.MessageType_GROUP_FILE:
		return "[文件]"
	case common.MessageType_CHAT_LOCATION:
		return "[位置]"
	case common.MessageType_CHAT_CUSTOM:
		return "[自定义]"
	default:
		return "[消息]"
	}
}
