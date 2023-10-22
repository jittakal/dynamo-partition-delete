FROM golang:1.21-alpine

LABEL "io.jittakal"="Jitendra Takalkar" 
LABEL version="0.1"
LABEL description="Amazon DynamoDB Delete Partition Utility"

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN go build -o /ddbctl /app/cmd/main.go

CMD [ "/ddbctl" ]