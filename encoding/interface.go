package encoding

type Encoding interface {
	String() string
	Extension() string
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

type EncodingRegistry interface {
	AddNew(Encoding)
	Get(string) (Encoding, bool)
}
