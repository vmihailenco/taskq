package azsqs

import (
	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-msgqueue/msgqueue"
)

type Manager struct {
	accountId  string
	sqsSession *session.Session
	sqsClient  *sqs.SQS
}

func (m *Manager) NewQueue(opt *msgqueue.Options) *Queue {
	return NewQueue(m.sqsClient, m.accountId, opt)
}

func (m *Manager) Queues() []*Queue {
	return Queues()
}

func CreateCredentials(id, secret, token string) *aws_credentials.Credentials {
	return aws_credentials.NewStaticCredentials(id, secret, token)
}

func NewSQSManager(region, accountId string, credentials *aws_credentials.Credentials) (*Manager, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials,
	})
	if err != nil {
		return nil, err
	}

	return &Manager{
		accountId:  accountId,
		sqsSession: sess,
		sqsClient:  sqs.New(sess),
	}, nil
}
