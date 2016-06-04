package ironmq

type Options struct {
	Host      string
	ProjectId string
	Token     string
	Name      string
}

func (opt *Options) baseUrl() string {
	return "https://" + opt.Host + "/3/projects/" + opt.ProjectId
}

func (opt *Options) url(path string) string {
	return opt.baseUrl() + path + "?oauth=" + opt.Token
}
