package common

import "strings"

func GetConversationType(sessionId string) ConversationType {
	if IsGroupSession(sessionId) {
		return ConversationType_CONVERSATION_TYPE_GROUP
	}
	return ConversationType_CONVERSATION_TYPE_PRIVATE
}

func IsGroupSession(sessionId string) bool {
	return strings.HasPrefix(sessionId, "group")
}

func IsPrivateSession(sessionId string) bool {
	return strings.HasPrefix(sessionId, "private")
}

func IsChatMessage(t MessageType) bool {
	return t >= MessageType_CHAT_TEXT && t <= MessageType_CHAT_CUSTOM
}

func IsGroupMessage(t MessageType) bool {
	return t >= MessageType_GROUP_TEXT && t <= MessageType_GROUP_NOTICE
}

func IsNotifyMessage(t MessageType) bool {
	return t >= MessageType_NOTIFICATION && t <= MessageType_APPLY_REJECT
}