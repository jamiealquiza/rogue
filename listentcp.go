// 2015 Jamie Alquiza

// Yanked the tcp listener guts from Ascender. This is purely for testing.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
)

var maxMsgSize = 256 * 1024

// Listens for messages, reqHandler goroutine dispatched for each.
func listenTcp() {
	log.Printf("Rogue TCP listener started: %s:%s\n",
		config.addr,
		config.port)
	server, err := net.Listen("tcp", config.addr+":"+config.port)
	if err != nil {
		log.Fatalf("Listener error: %s\n", err)
	}
	defer server.Close()
	// Connection handler loop.
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Printf("Listener down: %s\n", err)
			continue
		}
		go reqHandler(conn)
	}
}

// Receives messages from 'listener' & sends over 'messageIncomingQueue'.
func reqHandler(conn net.Conn) {
	// Read messages and split on newline.
	var reqBuf bytes.Buffer
	io.Copy(&reqBuf, conn)
	messages := bufio.NewScanner(&reqBuf)

	for messages.Scan() {
		m := messages.Bytes()

		// Drop message and respond if the 'batchBuffer' is at capacity.
		if len(messageIncomingQueue) >= config.queuecap {
			status := response(503, 0, "message queue full")
			conn.Write(status)
		} else {
			// Queue message and send response back to client.
			switch {
			case len(m) > maxMsgSize:
				status := response(400, len(m), "exceeds message size limit")
				conn.Write(status)
				messageIncomingQueue <- m[:maxMsgSize]
				conn.Close()
			case string(m) == "\n":
				status := response(204, len(m), "received empty message")
				conn.Write(status)
			default:
				status := response(200, len(m), "received")
				conn.Write(status)
				messageIncomingQueue <- m
			}
		}
	}
	conn.Close()
}

// Generate response codes.
func response(code int, bytes int, info string) []byte {
	message := fmt.Sprintf("%d|%d|%s\n", code, bytes, info)
	return []byte(message)
}
