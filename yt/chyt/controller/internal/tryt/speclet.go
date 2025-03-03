package tryt

type Speclet struct {
	CPU    *float64 `yson:"cpu"`
	Memory *uint64  `yson:"memory"`

	DockerImage string `yson:"docker_image"`

	TransferType      string `yson:"transfer_type"`
	SourceType        string `yson:"src_type"`
	SourceParams      any    `yson:"src_params"`
	DestinationType   string `yson:"dst_type"`
	DestinationParams any    `yson:"dst_params"`
}

const (
	gib           = 1024 * 1024 * 1024
	DefaultCPU    = 0.1
	DefaultMemory = 0.5 * gib
)

func (s *Speclet) CPUOrDefault() float64 {
	if s.CPU != nil {
		return *s.CPU
	}
	return DefaultCPU
}

func (s *Speclet) MemoryOrDefault() uint64 {
	if s.Memory != nil {
		return *s.Memory
	}
	return DefaultMemory
}

func (s *Speclet) Command() string {
	switch s.TransferType {
	case "INCREMENT_ONLY", "SNAPSHOT_AND_INCREMENT":
		return "replicate"
	default:
		return "activate"
	}
}
