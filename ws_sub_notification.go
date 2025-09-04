package hyperliquid

//go:generate easyjson -all

type NotificationSubscriptionParams struct {
	User string
}

func (w *WebsocketClient) Notification(
	params NotificationSubscriptionParams,
	callback func(Notification, error),
) (*Subscription, error) {
	payload := remoteNotificationSubscriptionPayload{
		Type: ChannelNotification,
		User: params.User,
	}

	return w.subscribe(payload, func(msg any) {
		notification, ok := msg.(Notification)
		if !ok {
			callback(Notification{}, ErrInvalidMessageType)
			return
		}

		callback(notification, nil)
	})
}
