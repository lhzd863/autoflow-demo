package module

type MetaJobImageBean struct {
	ImageId    string `json:"imageid"`
	Tag        string `json:"tag"`
	CreateTime string `json:"createtime"`
	DbStore    string `json:"dbstore"`
        User       string `json:"user"`
	Enable     string `json:"enable"`
}
