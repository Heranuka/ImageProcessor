package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Http     HttpConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
	Postgres DBConfig
}

type KafkaConfig struct {
	BrokerList                    []string      `env:"KAFKA_BROKER_LIST"`
	Topic                         string        `env:"KAFKA_TOPIC"`
	InitialBackoff                time.Duration `yaml:"initial_backoff"`
	MaxRetries                    int           `yaml:"max_retries"`
	TreatUnmarshalErrorAsCritical bool          `yaml:"treatUnmarshalErrorAsCritical"`
	ConsumerGroup                 string        `env:"CONSUMER_GROUP" `
}

type HttpConfig struct {
	Port            string        `env:"HTTP_PORT"`
	ReadTimeout     time.Duration `env:"HTTP_READ_TIMEOUT"`
	WriteTimeout    time.Duration `env:"HTTP_WRITE_TIMEOUT"`
	ShutdownTimeout time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT"`
}

type PostgresConfig struct {
	Host     string `env:"POSTGRES_HOST"`
	Port     int    `env:"POSTGRES_PORT"`
	Database string `env:"POSTGRES_DATABASE"`
	User     string `env:"POSTGRES_USER"`
	Password string `env:"POSTGRES_PASSWORD"`
	SSLMode  string `env:"POSTGRES_SSL_MODE"`
}

type RedisConfig struct {
	Addr     string `env:"REDIS_ADDR"`
	Password string `env:"REDIS_PASSWORD"`
	DBRedis  int    `env:"REDIS_DBREDIS"`
}

type DBConfig struct {
	Master PostgresConfig
	Slaves []PostgresConfig

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found, continuing with system environment variables")
	}

	cfg := Config{}

	cfg.Http.Port = os.Getenv("HTTP_PORT")

	readTimeout := os.Getenv("HTTP_READ_TIMEOUT")
	if readTimeout == "" {
		readTimeout = "10s"
	}
	cfg.Http.ReadTimeout, _ = time.ParseDuration(readTimeout)

	writeTimeout := os.Getenv("HTTP_WRITE_TIMEOUT")
	if writeTimeout == "" {
		writeTimeout = "10s"
	}
	cfg.Http.WriteTimeout, _ = time.ParseDuration(writeTimeout)

	shutdownTimeout := os.Getenv("HTTP_SHUTDOWN_TIMEOUT")
	if shutdownTimeout == "" {
		shutdownTimeout = "10s"
	}
	cfg.Http.ShutdownTimeout, _ = time.ParseDuration(shutdownTimeout)

	cfg.Postgres.Master.Host = os.Getenv("POSTGRES_HOST")
	cfg.Postgres.Master.Port, _ = strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	cfg.Postgres.Master.Database = os.Getenv("POSTGRES_DATABASE")
	cfg.Postgres.Master.User = os.Getenv("POSTGRES_USER")
	cfg.Postgres.Master.Password = os.Getenv("POSTGRES_PASSWORD")
	cfg.Postgres.Master.SSLMode = os.Getenv("POSTGRES_SSL_MODE")
	brokers := strings.Split(os.Getenv("KAFKA_BROKER_LIST"), ",")
	cfg.Kafka.Topic = os.Getenv("KAFKA_TOPIC")
	initialBackoff, _ := time.ParseDuration(os.Getenv("INITIAL_BACKOFF"))
	cfg.Kafka.BrokerList = brokers
	cfg.Kafka.InitialBackoff = initialBackoff
	maxRetries, _ := strconv.Atoi(os.Getenv("MAX_RETRIES"))
	cfg.Kafka.MaxRetries = maxRetries
	cfg.Kafka.ConsumerGroup = os.Getenv("CONSUMER_GROUP")

	log.Printf("Config loaded: %+v\n", cfg)

	return &cfg, nil
}
