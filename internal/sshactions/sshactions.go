package sshactions

import (
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"time"
)

func EjecutarUnComando(host string, port string, usuario string, comando string) {
	// Comando a ejecutar
	cmd := comando
	// Obtencion de la clave privada
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	key, err := ioutil.ReadFile(usr.HomeDir + "/.ssh/id_rsa")
	if err != nil {
		log.Fatalf("unable to read private key: %v", err)
	}
	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Fatalf("unable to parse private key: %v", err)
	}
	config := &ssh.ClientConfig{
		User: usuario,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		// allow any host key to be used (non-prod)
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),

		// verify host public key
		// HostKeyCallback: ssh.FixedHostKey(hostKey),
		// optional host key algo list
		HostKeyAlgorithms: []string{
			ssh.KeyAlgoRSA,
			ssh.KeyAlgoDSA,
			ssh.KeyAlgoECDSA256,
			ssh.KeyAlgoECDSA384,
			ssh.KeyAlgoECDSA521,
			ssh.KeyAlgoED25519,
		},
		// optional tcp connect timeout
		Timeout: 5 * time.Second,
	}
	// Conexion
	client, err := ssh.Dial("tcp", host+":22", config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	// Start session
	sess, err := client.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	defer sess.Close()
	// setup standard out and error
	// uses writer interface
	sess.Stdout = os.Stdout
	//sess.Stderr = os.Stderr
	// Ejecutar el comando
	err = sess.Run(cmd)
}
