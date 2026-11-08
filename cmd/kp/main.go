package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"kafka-pilot/internal/app"
	"kafka-pilot/internal/config"
	"kafka-pilot/internal/produce"
)

var (
	// Global flags
	flagProfile  string
	flagSession  string
	flagIntent   string
	flagFormat   string
	flagApp      *app.App
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "kp",
		Short: "Kafka Pilot - AI Agent & human dual-end Kafka data tool",
		Long:  "Kafka Pilot: Query, produce, and audit Kafka data with CLI and Web UI.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if isHelpCommand(cmd) {
				return nil
			}
			a, err := app.New()
			if err != nil {
				return fmt.Errorf("init app: %w", err)
			}
			flagApp = a
			return nil
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			if flagApp != nil {
				flagApp.Close()
			}
		},
	}
	rootCmd.PersistentFlags().StringVar(&flagProfile, "profile", "", "profile name (default: use configured default)")
	rootCmd.PersistentFlags().StringVar(&flagSession, "session", "", "session ID")
	rootCmd.PersistentFlags().StringVar(&flagIntent, "intent", "", "AI-declared intent for audit")
	rootCmd.PersistentFlags().StringVar(&flagFormat, "format", "json", "output format: json|table")

	// Register commands
	rootCmd.AddCommand(
		newProfileCmd(),
		newTopicCmd(),
		newGroupCmd(),
		newScanCmd(),
		newProduceCmd(),
		newSessionCmd(),
		newAuditCmd(),
		newSchemaCmd(),
		newServeCmd(),
		newDesktopCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func isHelpCommand(cmd *cobra.Command) bool {
	return cmd.Name() == "help" || (cmd.Flags().Changed("help"))
}

// output prints result as JSON or table
func output(v interface{}) {
	if flagFormat == "table" {
		printTable(v)
		return
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal error: %v\n", err)
		os.Exit(5)
	}
	fmt.Println(string(b))
}

func outputError(code int, message string, hint string) {
	errJSON := map[string]interface{}{
		"code":    code,
		"message": message,
	}
	if hint != "" {
		errJSON["hint"] = hint
	}
	b, _ := json.MarshalIndent(errJSON, "", "  ")
	fmt.Fprintln(os.Stderr, string(b))
	os.Exit(code)
}

func handleErr(err error) {
	if err == nil {
		return
	}
	// Classify error
	msg := err.Error()
	if strings.Contains(msg, "connection") || strings.Contains(msg, "TCP") || strings.Contains(msg, "SASL") || strings.Contains(msg, "TLS") {
		outputError(3, msg, "check profile configuration and Kafka cluster availability")
	}
	if strings.Contains(msg, "invalid") || strings.Contains(msg, "required") || strings.Contains(msg, "exceeds") {
		outputError(2, msg, "check command parameters")
	}
	outputError(5, msg, "")
}

// newOpCtx creates an op context for audit
func newOpCtx(command string) *app.OpContext {
	source := "ai"
	actor := app.GetActor()
	return flagApp.NewOpContext(flagSession, source, actor, flagIntent, command)
}

// --- Profile Commands ---

func newProfileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage connection profiles",
	}
	cmd.AddCommand(profileListCmd(), profileAddCmd(), profileUseCmd(), profileShowCmd(), profileRemoveCmd())
	return cmd
}

func profileListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all profiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			profiles := flagApp.ListProfiles()
			output(profiles)
			return nil
		},
	}
}

func profileAddCmd() *cobra.Command {
	var (
		name           string
		bootstrap      string
		saslMechanism  string
		saslUsername   string
		saslPassword   string
		tlsEnabled     bool
		tlsCA          string
		tlsSkipVerify  bool
	)
	c := &cobra.Command{
		Use:   "add",
		Short: "Add a new profile",
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if bootstrap == "" {
				return fmt.Errorf("--bootstrap-servers is required")
			}
			p := config.Profile{
				Name:             name,
				BootstrapServers: config.ParseBootstrapServers(bootstrap),
			}
			if saslMechanism != "" {
				p.SASL = &config.SASL{
					Mechanism: saslMechanism,
					Username:  saslUsername,
					Password:  saslPassword,
				}
			}
			if tlsEnabled {
				p.TLS = &config.TLS{
					CACert:     tlsCA,
					SkipVerify: tlsSkipVerify,
				}
			}
			if err := flagApp.AddProfile(p); err != nil {
				return err
			}
			output(map[string]string{"status": "added", "profile": name})
			return nil
		},
	}
	c.Flags().StringVar(&name, "name", "", "profile name")
	c.Flags().StringVar(&bootstrap, "bootstrap-servers", "", "comma-separated broker addresses")
	c.Flags().StringVar(&saslMechanism, "sasl-mechanism", "", "SASL mechanism: plain|scram-sha-256|scram-sha-512")
	c.Flags().StringVar(&saslUsername, "sasl-username", "", "SASL username")
	c.Flags().StringVar(&saslPassword, "sasl-password", "", "SASL password")
	c.Flags().BoolVar(&tlsEnabled, "tls", false, "enable TLS")
	c.Flags().StringVar(&tlsCA, "tls-ca", "", "CA certificate PEM file path")
	c.Flags().BoolVar(&tlsSkipVerify, "tls-skip-verify", false, "skip server certificate verification")
	return c
}

func profileUseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use [name]",
		Short: "Set default profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flagApp.SetDefaultProfile(args[0]); err != nil {
				return err
			}
			output(map[string]string{"status": "default_set", "profile": args[0]})
			return nil
		},
	}
}

func profileShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show [name]",
		Short: "Show profile details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := flagApp.Config.Get(args[0])
			if err != nil {
				return err
			}
			// Mask password
			if p.SASL != nil && p.SASL.Password != "" {
				p.SASL.Password = "****"
			}
			output(p)
			return nil
		},
	}
}

func profileRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove [name]",
		Short: "Remove a profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flagApp.RemoveProfile(args[0]); err != nil {
				return err
			}
			output(map[string]string{"status": "removed", "profile": args[0]})
			return nil
		},
	}
}

// --- Topic Commands ---

func newTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Browse topics",
	}
	cmd.AddCommand(topicListCmd(), topicDescribeCmd())
	return cmd
}

func topicListCmd() *cobra.Command {
	var filter string
	c := &cobra.Command{
		Use:   "list",
		Short: "List topics (minimal: name, partitions, latest offset)",
		RunE: func(cmd *cobra.Command, args []string) error {
			opCtx := newOpCtx(fmt.Sprintf("kp topic list --filter %s", filter))
			topics, err := flagApp.ListTopics(context.Background(), flagProfile, filter)
			if err != nil {
				flagApp.FinishOp(opCtx, "error", nil, nil, err)
				return err
			}
			flagApp.FinishOp(opCtx, "ok", map[string]interface{}{"count": len(topics)}, nil, nil)
			output(topics)
			return nil
		},
	}
	c.Flags().StringVar(&filter, "filter", "", "topic name filter (substring)")
	return c
}

func topicDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe [topic]",
		Short: "Describe a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opCtx := newOpCtx(fmt.Sprintf("kp topic describe %s", args[0]))
			topic, err := flagApp.DescribeTopic(context.Background(), flagProfile, args[0])
			if err != nil {
				flagApp.FinishOp(opCtx, "error", nil, nil, err)
				return err
			}
			flagApp.FinishOp(opCtx, "ok", nil, nil, nil)
			output(topic)
			return nil
		},
	}
}

// --- Group Commands ---

func newGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Consumer group operations",
	}
	cmd.AddCommand(groupListCmd(), groupLagCmd())
	return cmd
}

func groupListCmd() *cobra.Command {
	var topic string
	c := &cobra.Command{
		Use:   "list",
		Short: "List consumer groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			groups, err := flagApp.ListGroups(context.Background(), flagProfile)
			if err != nil {
				return err
			}
			output(groups)
			return nil
		},
	}
	c.Flags().StringVar(&topic, "topic", "", "filter by topic")
	return c
}

func groupLagCmd() *cobra.Command {
	var topic string
	c := &cobra.Command{
		Use:   "lag [group]",
		Short: "Show consumer group lag",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lag, err := flagApp.GroupLag(context.Background(), flagProfile, args[0], topic)
			if err != nil {
				return err
			}
			output(lag)
			return nil
		},
	}
	c.Flags().StringVar(&topic, "topic", "", "filter by topic")
	return c
}

// --- Scan Command ---

func newScanCmd() *cobra.Command {
	var (
		topic       string
		partitions  string
		last        int
		since       string
		until       string
		offsetFrom  int64
		offsetTo    int64
		key         string
		keyRegex    string
		keyword     string
		ignoreCase  bool
		expr        string
		timeoutSec  int
		outputFile  string
	)
	c := &cobra.Command{
		Use:   "scan",
		Short: "Scan messages with filters",
		RunE: func(cmd *cobra.Command, args []string) error {
			if topic == "" {
				return fmt.Errorf("--topic is required")
			}

			cmdStr := fmt.Sprintf("kp scan --topic %s", topic)
			opCtx := newOpCtx(cmdStr)

			req := &app.ScanRequest{
				Profile: flagProfile,
				Topic:   topic,
				Last:    last,
			}

			if partitions != "" {
				for _, p := range strings.Split(partitions, ",") {
					p = strings.TrimSpace(p)
					if p == "" {
						continue
					}
					n, err := strconv.ParseInt(p, 10, 32)
					if err != nil {
						return fmt.Errorf("invalid partition %q: %w", p, err)
					}
					req.Partitions = append(req.Partitions, int32(n))
				}
			}

			if since != "" {
				t, err := time.Parse(time.RFC3339, since)
				if err != nil {
					return fmt.Errorf("invalid --since format (use RFC3339): %w", err)
				}
				req.Since = t
				req.HasTimeWindow = true
			}
			if until != "" {
				t, err := time.Parse(time.RFC3339, until)
				if err != nil {
					return fmt.Errorf("invalid --until format (use RFC3339): %w", err)
				}
				req.Until = t
				req.HasTimeWindow = true
			}

			if cmd.Flags().Changed("offset-from") || cmd.Flags().Changed("offset-to") {
				req.OffsetFrom = offsetFrom
				req.OffsetTo = offsetTo
				req.HasOffsetRange = true
				req.Last = 0 // offset range and last are mutually exclusive
			}

			if last == 0 && !req.HasTimeWindow && !req.HasOffsetRange {
				last = 100 // default
				req.Last = 100
			}

			req.Key = key
			req.KeyRegex = keyRegex
			req.Keyword = keyword
			req.IgnoreCase = !cmd.Flags().Changed("ignore-case") || ignoreCase
			req.Expr = expr
			req.TimeoutSec = timeoutSec

			result, err := flagApp.Scan(context.Background(), opCtx, req)
			if err != nil {
				handleErr(err)
				return nil
			}

			if outputFile != "" {
				b, _ := json.MarshalIndent(result, "", "  ")
				return os.WriteFile(outputFile, b, 0o644)
			}
			output(result)
			return nil
		},
	}
	c.Flags().StringVar(&topic, "topic", "", "topic to scan (required)")
	c.Flags().StringVar(&partitions, "partition", "", "partitions to scan (comma-separated, e.g. 0,2)")
	c.Flags().IntVar(&last, "last", 100, "backscan count (回扫量语义, max 5000)")
	c.Flags().StringVar(&since, "since", "", "time window start (RFC3339)")
	c.Flags().StringVar(&until, "until", "", "time window end (RFC3339)")
	c.Flags().Int64Var(&offsetFrom, "offset-from", 0, "exact offset range start")
	c.Flags().Int64Var(&offsetTo, "offset-to", 0, "exact offset range end")
	c.Flags().StringVar(&key, "key", "", "exact key filter")
	c.Flags().StringVar(&keyRegex, "key-regex", "", "regex key filter")
	c.Flags().StringVar(&keyword, "keyword", "", "value keyword filter (substring)")
	c.Flags().BoolVar(&ignoreCase, "ignore-case", true, "case-insensitive keyword (default true)")
	c.Flags().StringVar(&expr, "expr", "", "jq expression filter")
	c.Flags().IntVar(&timeoutSec, "timeout", 30, "scan timeout in seconds")
	c.Flags().StringVar(&outputFile, "output", "", "write output to file")
	return c
}

// --- Produce Command ---

func newProduceCmd() *cobra.Command {
	var (
		topic       string
		key         string
		value       string
		valueFile   string
		keyFile     string
		headers     []string
		partition   int32
		script      string
		count       int
		concurrency int
		rate        int
		dryRun      bool
	)
	c := &cobra.Command{
		Use:   "produce",
		Short: "Produce messages (single or script batch)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if topic == "" {
				return fmt.Errorf("--topic is required")
			}

			cmdStr := fmt.Sprintf("kp produce --topic %s", topic)
			opCtx := newOpCtx(cmdStr)

			if script != "" {
				// Batch mode (v0.3: goja JS script execution)
				req := &produce.BatchRequest{
					Topic:       topic,
					ScriptPath:  script,
					Count:       count,
					Concurrency: concurrency,
					Rate:        rate,
					DryRun:      dryRun,
				}
				result, err := flagApp.ProduceBatch(context.Background(), opCtx, flagProfile, req, nil)
				if err != nil {
					handleErr(err)
					return nil
				}
				output(result)
				return nil
			}

			// Single mode
			var keyBytes, valueBytes []byte
			if keyFile != "" {
				data, err := os.ReadFile(keyFile)
				if err != nil {
					return fmt.Errorf("read key-file: %w", err)
				}
				keyBytes = data
			} else if key != "" {
				keyBytes = []byte(key)
			}

			if valueFile != "" {
				data, err := os.ReadFile(valueFile)
				if err != nil {
					return fmt.Errorf("read value-file: %w", err)
				}
				valueBytes = data
			} else if value != "" {
				valueBytes = []byte(value)
			} else {
				return fmt.Errorf("--value or --value-file is required (for single produce)")
			}

			result, err := flagApp.ProduceSingle(context.Background(), opCtx, flagProfile, topic, keyBytes, valueBytes, headers, partition)
			if err != nil {
				handleErr(err)
				return nil
			}
			output(result)
			return nil
		},
	}
	c.Flags().StringVar(&topic, "topic", "", "target topic (required)")
	c.Flags().StringVar(&key, "key", "", "message key")
	c.Flags().StringVar(&value, "value", "", "message value")
	c.Flags().StringVar(&valueFile, "value-file", "", "read value from file")
	c.Flags().StringVar(&keyFile, "key-file", "", "read key from file")
	c.Flags().StringArrayVar(&headers, "header", nil, "header key=value (repeatable)")
	c.Flags().Int32Var(&partition, "partition", -1, "target partition (-1 = auto)")
	c.Flags().StringVar(&script, "script", "", "JS script file for batch produce")
	c.Flags().IntVar(&count, "count", 0, "total messages for batch")
	c.Flags().IntVar(&concurrency, "concurrency", 8, "concurrency for batch (1-32)")
	c.Flags().IntVar(&rate, "rate", 0, "rate limit msgs/sec (0=unlimited)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "preview without sending")
	return c
}

// --- Session Commands ---

func newSessionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "session",
		Short: "Session management",
	}
	cmd.AddCommand(sessionNewCmd(), sessionEndCmd(), sessionListCmd(), sessionShowCmd())
	return cmd
}

func sessionNewCmd() *cobra.Command {
	var label string
	c := &cobra.Command{
		Use:   "new",
		Short: "Create a new session",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := flagApp.CreateSession(label)
			if err != nil {
				return err
			}
			output(map[string]string{"sessionId": id, "label": label})
			return nil
		},
	}
	c.Flags().StringVar(&label, "label", "", "session label")
	return c
}

func sessionEndCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "end [id]",
		Short: "End a session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flagApp.EndSession(args[0]); err != nil {
				return err
			}
			output(map[string]string{"status": "ended", "sessionId": args[0]})
			return nil
		},
	}
}

func sessionListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List sessions",
		RunE: func(cmd *cobra.Command, args []string) error {
			sessions, err := flagApp.ListSessions()
			if err != nil {
				return err
			}
			output(sessions)
			return nil
		},
	}
}

func sessionShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show [id]",
		Short: "Show session details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sess, err := flagApp.Audit.GetSession(args[0])
			if err != nil {
				return err
			}
			ops, _ := flagApp.ListOps(args[0], "", 0)
			output(map[string]interface{}{"session": sess, "ops": ops})
			return nil
		},
	}
}

// --- Audit Commands ---

func newAuditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Audit operations",
	}
	cmd.AddCommand(auditListCmd(), auditShowCmd(), auditClearCmd())
	return cmd
}

func auditListCmd() *cobra.Command {
	var (
		session string
		source  string
		since   string
	)
	c := &cobra.Command{
		Use:   "list",
		Short: "List audit operations",
		RunE: func(cmd *cobra.Command, args []string) error {
			var sinceTs int64
			if since != "" {
				t, err := time.Parse(time.RFC3339, since)
				if err != nil {
					return fmt.Errorf("invalid --since: %w", err)
				}
				sinceTs = t.UnixMilli()
			}
			ops, err := flagApp.ListOps(session, source, sinceTs)
			if err != nil {
				return err
			}
			output(ops)
			return nil
		},
	}
	c.Flags().StringVar(&session, "session", "", "filter by session ID")
	c.Flags().StringVar(&source, "source", "", "filter by source: ai|human")
	c.Flags().StringVar(&since, "since", "", "filter since timestamp (RFC3339)")
	return c
}

func auditShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show [op-id]",
		Short: "Show operation details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			op, err := flagApp.GetOp(args[0])
			if err != nil {
				return err
			}
			output(op)
			return nil
		},
	}
}

func auditClearCmd() *cobra.Command {
	var before string
	c := &cobra.Command{
		Use:   "clear",
		Short: "Clear audit data",
		RunE: func(cmd *cobra.Command, args []string) error {
			var beforeTs int64
			if before != "" {
				t, err := time.Parse(time.RFC3339, before)
				if err != nil {
					return fmt.Errorf("invalid --before: %w", err)
				}
				beforeTs = t.UnixMilli()
			}
			if err := flagApp.ClearAudit(beforeTs); err != nil {
				return err
			}
			output(map[string]string{"status": "cleared"})
			return nil
		},
	}
	c.Flags().StringVar(&before, "before", "", "clear before timestamp (RFC3339)")
	return c
}

// --- Schema Command ---

func newSchemaCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "schema",
		Short: "Output machine-readable command schema (JSON)",
		RunE: func(cmd *cobra.Command, args []string) error {
			schema := map[string]interface{}{
				"commands": []string{
					"profile", "topic", "group", "scan", "produce",
					"session", "audit", "schema", "serve", "desktop",
				},
				"exit_codes": map[string]int{
					"ok":         0,
					"param_error": 2,
					"conn_error":  3,
					"timeout":    4,
					"internal":   5,
				},
			}
			output(schema)
			return nil
		},
	}
}

// --- Serve / Desktop Commands ---

func newServeCmd() *cobra.Command {
	var port int
	c := &cobra.Command{
		Use:   "serve",
		Short: "Start local HTTP service",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startServe(port)
		},
	}
	c.Flags().IntVar(&port, "port", 0, "port (0=auto)")
	return c
}

func newDesktopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "desktop",
		Short: "Start service + open desktop window",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startDesktop()
		},
	}
}

// Helper for serve/desktop
func printTable(v interface{}) {
	// Simple table output
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}
