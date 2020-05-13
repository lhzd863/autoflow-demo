package module

type MetaJobDependencyBean struct {
	Sys            string `yaml:"sys"`
	Job            string `yaml:"job"`
        DependencySys  string `yaml:"dependencysys"`
        DependencyJob  string `yaml:"dependencyjob"`
        Enable  string `yaml:"enable"`
}
