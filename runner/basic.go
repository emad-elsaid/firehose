package runner

type Basic struct{}

func (Basic) Run(f func()) {
	go f()
}
