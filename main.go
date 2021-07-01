package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const TOPICS_FILE = "topics.json"

type Topic struct {
	Topic      string
	Group      string
	Date       string
	Offset     int
	Partitions []int
	ResetMode  int `json:"reset_mode"`
}

type Topics struct {
	Env    string
	Topics []Topic
}

type Options string

const (
	EARLIEST  = "--to-earliest"
	LATEST    = "-to-latest"
	DATETIME  = "--to-datetime"
	OFFSET    = "--to-offset"
	ALLTOPICS = "--all-topics"
)

func getOptions() []string {
	return []string{"--to-earliest", "-to-latest", "--to-datetime", "--to-offset"}
}

func buildCommand(env *string, topic *Topic, broker *string) string {
	resetId := topic.ResetMode
	if resetId < 0 || resetId > len(getOptions())-1 {
		fmt.Printf("Reset option %d is unknown", resetId)
		return ""
	}
	resetMode := getOptions()[resetId]
	var (
		offset     string
		datetime   string
		allTopics  string
		partitions string
	)
	switch resetMode {
	case OFFSET:
		offset = fmt.Sprint(topic.Offset)
	case DATETIME:
		datetime = topic.Date
	}
	topicValue := fmt.Sprintf(topic.Topic)
	if len(topic.Partitions) > 0 {
		var stringPartitions []string
		for _, topicPartition := range topic.Partitions {
			stringPartitions = append(stringPartitions, strconv.Itoa(topicPartition))
		}
		allTopics = ALLTOPICS
		partitions = strings.Join(stringPartitions, ",")
		topicValue = fmt.Sprintf("%s:%s", topic.Topic, partitions)
	}
	secuConfigFile := "secu.build.config"
	if *env == "PROD" {
		secuConfigFile = "secu.prod.config"
	}
	return fmt.Sprintf("kafka-consumer-groups --bootstrap-server %s --group %s --reset-offsets %s %s %s %s %s --execute --command-config /opt/secrets/%s", *broker, topic.Group, resetMode, offset, datetime, allTopics, topicValue, secuConfigFile)

}

func resetConsumerGroup(env *string, topic *Topic, broker *string) {
	args := strings.Split(buildCommand(env, topic, broker), " ")
	command := args[0]
	cmd := exec.Command(command, args[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(out))
}

func main() {
	topicsFile, err := ioutil.ReadFile(TOPICS_FILE)
	if err != nil {
		panic(err)
	}
	var topics Topics
	err2 := json.Unmarshal(topicsFile, &topics)
	if err2 != nil {
		panic(err2)
	}
	broker := os.Getenv(fmt.Sprintf("BROKERS_%s", topics.Env))
	for _, topic := range topics.Topics {
		resetConsumerGroup(&topics.Env, &topic, &broker)
	}
}
