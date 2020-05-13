package err

type ClientErr struct {
	Err  string
	Desc string
}

// Error returns the machine readable form
func (e *ClientErr) Error() string {
	return e.Err
}

// Description return the human readable form
func (e *ClientErr) Description() string {
	return e.Desc
}

// NewClientErr creates a ClientErr with the supplied human and machine readable strings
func NewClientErr(err string, description string) *ClientErr {
	return &ClientErr{err, description}
}

