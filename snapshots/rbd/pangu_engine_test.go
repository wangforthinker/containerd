package rbd

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/sirupsen/logrus"
	"os/exec"
)

var(
	testPanguOpts = []string{"pangu_autodeploy_5.vipserver","pangu_autodeploy_5"}
	testHome = "/tmp/"
)

func getTestPanguEngine() (RBDEngine,error) {
	return newPanEngine(testHome, testPanguOpts)
}

func TestPanguCreateAndRemove(t *testing.T)  {
	var(
		id string = "123420102fffffaxxxx"
	)

	e,err := getTestPanguEngine()
	assert.Nil(t, err)

	newId, exists, err := e.Create(id , 2048, false)
	assert.Nil(t, err)
	assert.Equal(t, false, exists)
	assert.Equal(t, id, newId)

	logrus.Infof("newId is %s", newId)

	err = e.Remove(id)
	assert.Nil(t, err)
}

func TestPanguCloneAndMap(t *testing.T)  {
	var(
		cloneId string = "c5183829c43c4698634093dc38f9bee26d1b931dedeba71dbee984f42fe1270d"
		river_master string = "pangu2-md-normal.vipserver"
		strict_cluster string = "pangu2-md-normal"
		id string = "123420102fffffayyyyy"
	)

	e,err := newPanEngine(testHome, []string{river_master, strict_cluster})
	assert.Nil(t, err)

	newId,exist,err := e.Clone(id, cloneId, false, 2048)
	assert.Nil(t, err)
	assert.Equal(t, false, exist)
	assert.Equal(t, newId, id)

	mountPath := e.MountPoint(id)
	//map
	dev,err := e.Map(id)
	assert.Nil(t, err)

	logrus.Infof("map dev is %s", dev)
	_, err = exec.Command("mount", dev, mountPath).CombinedOutput()
	assert.Nil(t, err)

	_,err = exec.Command("umount", mountPath).CombinedOutput()
	assert.Nil(t, err)

	err = e.Unmap(id)
	assert.Nil(t, err)
}

