package rbd

type RbdDiskManager struct {

}

func NewRbdManager(root string) (*RbdDiskManager, error) {
	return &RbdDiskManager{},nil
}

func (m *RbdDiskManager) load() error {
	return nil
}

func (m *RbdDiskManager) Get(id string) (*Disk, error) {
	return nil, nil
}