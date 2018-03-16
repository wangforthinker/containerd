package rbd


type Disk struct {
	*RemoteDisk
	Path		string
	Mapped		bool
	Dev		string
	Parent		string
	ReadOnly 	bool
}

//remote disk metadata
type RemoteDisk struct {
	//as remote id
	Id      string `json:"id"`
	Cap     int64  `json:"cap"`
	Size    int64  `json:"size"`
	Pid     string `json:"pid,omitempty"`
	Fmat    bool   `json:"fmat"`
	Format  string `json:"format"`
	Digest  string `json:"digest"`
	Storage string `json:"storage"`
	Params  []string
}

