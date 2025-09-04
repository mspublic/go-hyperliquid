package hyperliquid

//go:generate easyjson -all

type AllMidsSubscriptionParams struct {
	Dex *string
}

func (w *WebsocketClient) AllMids(
	params AllMidsSubscriptionParams,
	callback func(AllMids, error),
) (*Subscription, error) {
	payload := remoteAllMidsSubscriptionPayload{
		Type: ChannelAllMids,
		Dex:  params.Dex,
	}

	return w.subscribe(payload, func(msg any) {
		allmids, ok := msg.(AllMids)
		if !ok {
			callback(AllMids{}, ErrInvalidMessageType)
			return
		}

		callback(allmids, nil)
	})
}
