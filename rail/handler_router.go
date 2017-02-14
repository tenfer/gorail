package rail

//定义下游处理方法映射
var HandlerRouter = map[string]func(cc *ChannelConfig) Handler{
	"file": NewFile,
	"http": NewHttp}
