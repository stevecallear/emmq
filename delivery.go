package emmq

// Delivery represents a message delivery
type Delivery struct {
	Key      Key
	Value    []byte
	exchange *Exchange
}

// Delete deletes the message
// If a message is not deleted then it will be re-delivered after the visibility timeout
func (d *Delivery) Delete() error {
	return d.exchange.delete(d.Key)
}
