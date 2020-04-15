package hdfs

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Property is the struct representation of hadoop configuration
// key value pair.
type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []Property `xml:"property"`
}

// HadoopConf represents a map of all the key value configutation
// pairs found in a user's hadoop configuration files.
type HadoopConf map[string]string

var defaultFS string

var errUnresolvedDefaultFS = errors.New("no defaultFS in configuration")
var errUnresolvedNamenode = errors.New("no namenode address in configuration")

// LoadHadoopConf returns a HadoopConf object representing configuration from
// the specified path, or finds the correct path in the environment. If
// path or the env variable HADOOP_CONF_DIR is specified, it should point
// directly to the directory where the xml files are. If neither is specified,
// ${HADOOP_HOME}/conf will be used.
func LoadHadoopConf(path string) HadoopConf {

	if path == "" {
		path = os.Getenv("HADOOP_CONF_DIR")
		if path == "" {
			path = filepath.Join(os.Getenv("HADOOP_HOME"), "conf")
		}
	}

	hadoopConf := make(HadoopConf)
	for _, file := range []string{"core-site.xml", "hdfs-site.xml"} {
		pList := propertyList{}
		f, err := ioutil.ReadFile(filepath.Join(path, file))
		if err != nil {
			continue
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			continue
		}

		for _, prop := range pList.Property {
			hadoopConf[prop.Name] = prop.Value
		}
	}

	return hadoopConf
}

// Namenodes returns the namenode hosts present in the configuration. The
// returned slice will be sorted and deduped.
func (conf HadoopConf) Namenodes(givenFS string) ([]string, error) {
	nns := make(map[string]bool)
	var defaultFsName string
	if givenFS == "" {
		// find fs name first
		for key, value := range conf {
			if key == "fs.defaultFS" {
				defaultFsName = strings.TrimPrefix(value, "hdfs://")
			}
		}
		if defaultFsName == "" {
			return nil, errUnresolvedDefaultFS
		}
		defaultFS = defaultFsName
	} else {
		defaultFS = givenFS
	}

	// extract default FS cluster
	for key, value := range conf {
		k := fmt.Sprintf("dfs.namenode.rpc-address.%s.", defaultFsName)
		if strings.HasPrefix(key, k) {
			nns[value] = true
		}
	}

	if len(nns) == 0 {
		return nil, errUnresolvedNamenode
	}

	keys := make([]string, 0, len(nns))
	for k, _ := range nns {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys, nil
}

func GetDefaultFS() string {
	return defaultFS
}
