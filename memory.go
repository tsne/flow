package flow

func alloc(n int, buf []byte) []byte {
	if n <= cap(buf) {
		return buf[:n]
	}
	return make([]byte, n)
}
