package mets

import (
	"log"
	"os"
	"strings"

	"github.com/hashicorp/logutils"
)

func initLogger() {
	minLevel := "INFO"
	if l := os.Getenv("METS_LOG_LEVEL"); l != "" {
		minLevel = strings.ToUpper(l)
	}

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"},
		MinLevel: logutils.LogLevel(minLevel),
		Writer:   os.Stderr,
	}

	log.SetOutput(filter)
}
