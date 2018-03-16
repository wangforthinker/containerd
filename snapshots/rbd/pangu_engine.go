package rbd

import (
	"fmt"
	"os/exec"
	"time"
	"encoding/json"
	"strings"
	"github.com/Sirupsen/logrus"
)

const(
	PanguRBDEngine = "pangu"
)

func init()  {
	RegisterRBDEngine(PanguRBDEngine, newPanEngine)
}

type Pan struct {
	Home                string
	StrictClusterName   string
	RiverMasterLocation string
	Prefix              string
}

//opts should be river_master_location and strict_cluster_name
func newPanEngine(home string, opts []string) (RBDEngine, error) {
	p := &Pan{}
	p.Home = home
	//for _, one := range opts {
	//	arr := strings.SplitN(one, "=", 2)
	//	if len(arr) == 2 && arr[0] == "cluster" {
	//		p.StrictClusterName = arr[1]
	//	} else if len(arr) == 2 && arr[0] == "river" {
	//		p.RiverMasterLocation = arr[1]
	//	} else if len(arr) == 2 && arr[0] == "prefix" {
	//		p.Prefix = arr[1]
	//	}
	//}

	if len(opts) < 2 {
		return nil, fmt.Errorf("opts is not set")
	}

	p.RiverMasterLocation = opts[0]
	p.StrictClusterName = opts[1]

	if p.StrictClusterName == "" || p.RiverMasterLocation == "" {
		return nil,fmt.Errorf("cluster and river location are both required %s %s",
			p.StrictClusterName, p.RiverMasterLocation)
	}
	if p.Prefix == "" {
		p.Prefix = "/apsara/tdc/"
	}

	return p, nil
}

func (p *Pan) name(id string) string {
	return id
}

func (p *Pan) river() string {
	return fmt.Sprintf("--river_master_location=%s", p.RiverMasterLocation)
}

func (p *Pan) cluster() string {
	return fmt.Sprintf("--strict_cluster_name=%s", p.StrictClusterName)
}

func (p *Pan) uuid(id string) string {
	return fmt.Sprintf("--uuid=%s", id)
}

func (p *Pan) size(size int) string {
	return fmt.Sprintf("--device_size=%d", size)
}

type ErrResult struct {
	Errorcode string `json:"errorcode"`
}

func (p *Pan) Info(id string) bool {
	args := []string{"lsbd-query-device", p.uuid(id), p.river()}
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), args...)
	logrus.Infof("run command %s %v", fmt.Sprintf("%stdc_admin", p.Prefix), args)
	if er, err := runWithTimeout(cmd, time.Minute); err != nil {
		logrus.Warnf("%stdc_admin lsbd-query-device %s error %v %s %s", p.Prefix, id, err, er.Stdout, er.Stderr)
		return false
	} else {
		logrus.Infof("lsbd-query-device %s out %s %s", id, er.Stdout, er.Stderr)
		var errCode ErrResult
		if err := json.Unmarshal([]byte(er.Stdout), &errCode); err != nil {
			logrus.Errorf("unmarshal result of lsbd-query-device error. %s %v", er.Stdout, err)
			return false
		}
		return errCode.Errorcode == "0"
	}
}

func (p *Pan) Create(id string, megaSize int, ignoreExist bool) (string, bool, error) {
	remoteId := p.name(id)
	if p.Info(remoteId) {
		return remoteId, true, nil
	}

	args := []string{"lsbd-create", p.size(megaSize), p.uuid(remoteId), p.cluster(), p.river()}
	logrus.Infof("command: %s%s %v", p.Prefix, "tdc_admin", args)
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), args...)
	rs, e := runWithTimeout(cmd, time.Minute)
	if rs != nil {
		if rs.ExitStatus != 0 {
			logrus.Errorf("create disk %s exit %d %s %s", remoteId, rs.ExitStatus, rs.Stdout, rs.Stderr)
			return "", false, fmt.Errorf("Disk already exists %s", id)
		}
	} else if e != nil {
		logrus.Errorf("create disk ex %s %v %s %s", remoteId, e, rs.Stdout, rs.Stderr)
		return "", false, e
	}
	logrus.Infof("create rw layer %s, stdout %s, stderr %s, exit code %d", id,
		rs.Stdout, rs.Stderr, rs.ExitStatus)
	return remoteId, false, nil
}

func (p *Pan) Remove(id string) error {
	args := []string{"lsbd-remove", p.uuid(p.name(id)), p.river()}
	logrus.Infof("command: %s%s %v", p.Prefix, "tdc_admin", args)
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), args...)
	rs, _ := runWithTimeout(cmd, time.Minute)
	if rs.ExitStatus != 0 {
		logrus.Errorf("Destroying disk %q: %v (%v) %s", id, rs, rs.Stdout, rs.Stderr)
		return fmt.Errorf("Destroying disk %q: %v (%v) %s", id, rs, rs.Stdout, rs.Stderr)
	}
	logrus.Infof("rm remote layer %s, stdout %s, stderr %s, exit code %d", id,
		rs.Stdout, rs.Stderr, rs.ExitStatus)
	return nil
}

func (p *Pan) Clone(id, parent string, ignoreExist bool, size int) (string, bool, error) {
	remoteId := p.name(id)
	if p.Info(remoteId) {
		return remoteId, true, nil
	}
	args := []string{"lsbd-create", p.uuid(remoteId),
		fmt.Sprintf("--snapshot_uuid=%s", parent), p.cluster(), p.size(10240), p.river()}
	logrus.Infof("command: %s%s %v", p.Prefix, "tdc_admin", args)
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), args...)
	rs, e := runWithTimeout(cmd, time.Minute)
	if e != nil {
		logrus.Errorf("clone disk error %s %v %s %s", id, e, rs.Stdout, rs.Stderr)
		return "", false, e
	}
	if rs.ExitStatus != 0 {
		logrus.Errorf("Cloning snapshot error (id %q, snapshot %q): %v", id, parent, e)
		newerr := fmt.Errorf("Cloning snapshot error (id %q, snapshot %q): %v", id, parent, e)
		return "", false, newerr
	}
	logrus.Infof("clone snapshot %s, stdout %s, stderr %s, exit code %d", id,
		rs.Stdout, rs.Stderr, rs.ExitStatus)
	return remoteId, false, nil
}

func (p *Pan) Map(id string) (string, error) {
	logrus.Infof("command: %s%s %s %s %s", p.Prefix, "tdc_admin", "lsbd-load", p.uuid(p.name(id)), p.river())
	retries := 0
	retry:
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), "lsbd-load", p.uuid(p.name(id)), p.river())
	er, err := runWithTimeout(cmd, time.Minute)
	if retries < 10 && err != nil {
		logrus.Errorf("Error mapping image: %v (%v) (%v). Retrying. %s %s", id, er, err, er.Stdout, er.Stderr)
		retries++
		goto retry
	}

	if err != nil || er.ExitStatus != 0 {
		logrus.Errorf("Could not map %q: %v (%v) (%v)", id, er, err, er.Stderr)
		return "", fmt.Errorf("Could not map %q: %v (%v) (%v)", id, er, err, er.Stderr)
	}
	ret := map[string]string{}
	if err := json.Unmarshal([]byte(er.Stdout), &ret); err != nil {
		return "", err
	}
	dev := strings.TrimSpace(ret["msg"])
	logrus.Infof("dev of %s is %s", id, dev)
	return dev, nil
}

func (p *Pan) Unmap(id string) error {
	args := []string{"lsbd-unload", p.uuid(p.name(id)), p.river()}
	logrus.Infof("command: %s%s %v", p.Prefix, "tdc_admin", args)
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), args...)
	rs, e := runWithTimeout(cmd, time.Minute)
	if e != nil || rs.ExitStatus != 0 {
		logrus.Errorf("could not unmap device %s: %v (%v) (%v) %s", id, rs, e, rs.Stderr, rs.Stdout)
		return fmt.Errorf("Could not unmap device: %s: %v (%v) (%v) %s", id, rs, e, rs.Stderr, rs.Stdout)
	}
	logrus.Infof("unmap rw layer %s, stdout %s, stderr %s, exit code %d", id,
		rs.Stdout, rs.Stderr, rs.ExitStatus)
	return nil
}

func (p *Pan) MountPoint(id string) string {
	return fmt.Sprintf("%s/pangu/%s", p.Home, id)
}

func (p *Pan) Name() string {
	return "pangu"
}

func (p *Pan) Params() (arr []string) {
	return []string{p.RiverMasterLocation, p.StrictClusterName}
}

func (p *Pan) Mapped(id string) (bool, string, error) {
	args := []string{"lsbd-ls"}
	logrus.Infof("command: %s%s %v", p.Prefix, "tdc_admin", args)
	cmd := exec.Command(fmt.Sprintf("%stdc_admin", p.Prefix), args...)
	rs, e := runWithTimeout(cmd, time.Minute)
	if e != nil || rs.ExitStatus != 0 {
		logrus.Errorf("could not unmap device %s: %v (%v) (%v) %s", id, rs, e, rs.Stderr, rs.Stdout)
		return false, "", e
	}
	ret := map[string]interface{}{}
	e = json.Unmarshal([]byte(rs.Stdout), &ret)
	if e != nil {
		logrus.Errorf("unmarshal lsbd-ls result error. %v", e)
		return false, "", e
	}

	if ret["errorcode"].(string) != "0" {
		logrus.Errorf("get lsbd-ls result error. %s", rs.Stdout)
		return false, "", fmt.Errorf("get lsbd-ls result error. %s", rs.Stdout)
	}

	switch v := ret["msg"].(type) {
	case []interface{}:
		for _, one := range v {
			switch mm := one.(type) {
			case map[string]interface{}:
				if mm["uuid"].(string) == id {
					return true, mm["disk_path"].(string), nil
				}
			case map[string]string:
				if mm["uuid"] == id {
					return true, mm["disk_path"], nil
				}
			default:
				logrus.Errorf("get lsbd-ls result msg item error. %s", rs.Stdout)
				return false, "", fmt.Errorf("get lsbd-ls result msg item error. %s", rs.Stdout)
			}
		}
	default:
		logrus.Errorf("get lsbd-ls result msg error. %s", rs.Stdout)
		return false, "", fmt.Errorf("get lsbd-ls result msg error. %s", rs.Stdout)
	}

	return false, "", nil
}
