package nats

type optionError string

func (e optionError) Error() string {
	return "invalid nats option: " + string(e)
}
