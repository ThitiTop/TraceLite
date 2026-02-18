package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trace-lite/collector/internal/clickhouse"
	"trace-lite/collector/internal/config"
	"trace-lite/collector/internal/reconstruct"
	"trace-lite/collector/internal/server"
)

func main() {
	cfg := config.Load()
	ch := clickhouse.NewClient(cfg.ClickHouseDSN, cfg.ClickHouseDB)
	recon := reconstruct.New(ch, cfg.TraceWindow, cfg.FlushInterval)
	h := server.NewHandler(cfg.IngestToken, ch, recon)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", h.Healthz)
	mux.HandleFunc("/v1/ingest/logs", h.IngestLogs)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go recon.Run(ctx)

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	cert, err := loadOrCreateCert(cfg)
	if err != nil {
		log.Fatalf("tls cert: %v", err)
	}

	tlsLn := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})
	log.Printf("collector listening https://0.0.0.0%s", cfg.Addr)

	go func() {
		if err := srv.Serve(tlsLn); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	recon.FlushNow(shutdownCtx)
}

func loadOrCreateCert(cfg config.Config) (tls.Certificate, error) {
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		return tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	}
	if !cfg.TLSAutoSelfSigned {
		return tls.Certificate{}, os.ErrNotExist
	}
	return generateSelfSigned()
}

func generateSelfSigned() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	now := time.Now().UTC()
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject: pkix.Name{
			Organization: []string{"trace-lite-dev"},
			CommonName:   "collector",
		},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"collector", "localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return tls.X509KeyPair(certPEM, keyPEM)
}
