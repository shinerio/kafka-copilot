package config

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// Profile represents a Kafka cluster connection configuration.
type Profile struct {
	Name             string   `yaml:"name" json:"name"`
	BootstrapServers []string `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	SASL             *SASL    `yaml:"sasl,omitempty" json:"sasl,omitempty"`
	TLS              *TLS     `yaml:"tls,omitempty" json:"tls,omitempty"`
}

// SASL holds optional username/password authentication.
type SASL struct {
	Mechanism string `yaml:"mechanism" json:"mechanism"` // plain | scram-sha-256 | scram-sha-512
	Username  string `yaml:"username" json:"username"`
	Password  string `yaml:"password" json:"password"` // DPAPI-encrypted on Windows, base64 elsewhere
}

// TLS holds optional TLS configuration (one-way: client verifies server).
type TLS struct {
	CACert     string `yaml:"ca_cert,omitempty" json:"ca_cert,omitempty"`         // PEM file path
	SkipVerify bool   `yaml:"skip_verify,omitempty" json:"skip_verify,omitempty"` // skip server cert verification
}

// Config is the top-level config file structure.
type Config struct {
	DefaultProfile string    `yaml:"default_profile" json:"default_profile"`
	Profiles       []Profile `yaml:"profiles" json:"profiles"`
}

// Manager handles profile CRUD and persistence.
type Manager struct {
	mu     sync.RWMutex
	path   string
	config *Config
}

// NewManager creates a config manager using the resolved data directory
// (configurable; defaults to kp-data next to the executable).
func NewManager() (*Manager, error) {
	dir, err := DataDir()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "config.yaml")
	m := &Manager{path: path}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

// NewManagerWithPath creates a config manager with an explicit path (for testing).
func NewManagerWithPath(path string) (*Manager, error) {
	m := &Manager{path: path}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			m.config = &Config{}
			return nil
		}
		return fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config yaml: %w", err)
	}
	// Decrypt passwords
	for i := range cfg.Profiles {
		if cfg.Profiles[i].SASL != nil && cfg.Profiles[i].SASL.Password != "" {
			if pw, err := Decrypt(cfg.Profiles[i].SASL.Password); err == nil {
				cfg.Profiles[i].SASL.Password = pw
			}
		}
	}
	m.config = &cfg
	return nil
}

// cloneProfile returns a deep copy so callers can mask or encrypt a profile
// without mutating the in-memory (plaintext) source. The SASL/TLS pointers are
// copied too: otherwise masking/encrypting the copy would also rewrite the
// shared object that m.config still points at. That was a real bug -- a GET
// that masks the password (or a save that encrypts) silently corrupted the
// live in-memory password used for connections in the long-running serve
// process, so Web UI saves appeared to "not stick" while CLI (fresh process)
// kept working.
func cloneProfile(p Profile) Profile {
	q := p
	if p.SASL != nil {
		s := *p.SASL
		q.SASL = &s
	}
	if p.TLS != nil {
		t := *p.TLS
		q.TLS = &t
	}
	return q
}

func (m *Manager) save() error {
	// Deep copy: encrypt for disk WITHOUT mutating the in-memory plaintext.
	cp := Config{
		DefaultProfile: m.config.DefaultProfile,
		Profiles:       make([]Profile, len(m.config.Profiles)),
	}
	for i := range m.config.Profiles {
		cp.Profiles[i] = cloneProfile(m.config.Profiles[i])
		if cp.Profiles[i].SASL != nil && cp.Profiles[i].SASL.Password != "" {
			if enc, err := Encrypt(cp.Profiles[i].SASL.Password); err == nil {
				cp.Profiles[i].SASL.Password = enc
			}
		}
	}
	data, err := yaml.Marshal(&cp)
	if err != nil {
		return fmt.Errorf("marshal config yaml: %w", err)
	}
	if err := os.WriteFile(m.path, data, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// List returns all profiles.
func (m *Manager) List() []Profile {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Profile, 0, len(m.config.Profiles))
	for i := range m.config.Profiles {
		p := cloneProfile(m.config.Profiles[i])
		// Mask passwords in output
		if p.SASL != nil && p.SASL.Password != "" {
			p.SASL.Password = "****"
		}
		result = append(result, p)
	}
	return result
}

// Get returns a profile by name (with decrypted password).
func (m *Manager) Get(name string) (*Profile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.config.Profiles {
		if m.config.Profiles[i].Name == name {
			p := cloneProfile(m.config.Profiles[i])
			return &p, nil
		}
	}
	return nil, fmt.Errorf("profile %q not found", name)
}

// Add adds a new profile.
func (m *Manager) Add(p Profile) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.config.Profiles {
		if existing.Name == p.Name {
			return fmt.Errorf("profile %q already exists", p.Name)
		}
	}
	if p.SASL != nil && p.SASL.Password != "" {
		// Password comes in as plaintext from CLI/UI, keep it decrypted in memory
	}
	m.config.Profiles = append(m.config.Profiles, p)
	if m.config.DefaultProfile == "" {
		m.config.DefaultProfile = p.Name
	}
	return m.save()
}

// Update updates an existing profile.
func (m *Manager) Update(name string, p Profile) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for i := range m.config.Profiles {
		if m.config.Profiles[i].Name == name {
			if p.SASL != nil && p.SASL.Password == "****" {
				// Keep existing password if not changed
				p.SASL.Password = m.config.Profiles[i].SASL.Password
			}
			p.Name = name
			m.config.Profiles[i] = p
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("profile %q not found", name)
	}
	return m.save()
}

// Remove deletes a profile.
func (m *Manager) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := -1
	for i := range m.config.Profiles {
		if m.config.Profiles[i].Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("profile %q not found", name)
	}
	m.config.Profiles = append(m.config.Profiles[:idx], m.config.Profiles[idx+1:]...)
	if m.config.DefaultProfile == name {
		m.config.DefaultProfile = ""
		if len(m.config.Profiles) > 0 {
			m.config.DefaultProfile = m.config.Profiles[0].Name
		}
	}
	return m.save()
}

// SetDefault sets the default profile.
func (m *Manager) SetDefault(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for _, p := range m.config.Profiles {
		if p.Name == name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("profile %q not found", name)
	}
	m.config.DefaultProfile = name
	return m.save()
}

// Default returns the default profile name.
func (m *Manager) Default() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.DefaultProfile
}

// Resolve returns the profile to use: explicit name, or default.
func (m *Manager) Resolve(name string) (*Profile, error) {
	if name != "" {
		return m.Get(name)
	}
	def := m.Default()
	if def == "" {
		return nil, fmt.Errorf("no profile configured; use 'kp profile add' first")
	}
	return m.Get(def)
}

// ParseBootstrapServers splits a comma-separated string into a list.
func ParseBootstrapServers(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

// randomHex generates a random hex string of the given byte length.
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}
