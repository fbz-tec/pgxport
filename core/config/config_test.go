package config

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected Config
	}{
		{
			name: "default values when no env vars set",
			envVars: map[string]string{
				"DB_USER":   "",
				"DB_PASS":   "",
				"DB_HOST":   "",
				"DB_PORT":   "",
				"DB_NAME":   "",
				"DB_DRIVER": "",
			},
			expected: Config{
				DBDriver: DefaultDBDriver,
				DBUser:   DefaultDBUser,
				DBPass:   "",
				DBHost:   DefaultDBHost,
				DBPort:   DefaultDBPort,
				DBName:   DefaultDBName,
			},
		},
		{
			name: "custom values from env vars",
			envVars: map[string]string{
				"DB_USER": "testuser",
				"DB_PASS": "testpass",
				"DB_HOST": "testhost",
				"DB_PORT": "5433",
				"DB_NAME": "testdb",
			},
			expected: Config{
				DBDriver: DefaultDBDriver,
				DBUser:   "testuser",
				DBPass:   "testpass",
				DBHost:   "testhost",
				DBPort:   5433,
				DBName:   "testdb",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean environment
			os.Clearenv()

			// Set test env vars
			for k, v := range tt.envVars {
				if v != "" {
					os.Setenv(k, v)
				}
			}

			config := LoadConfig()

			if config.DBDriver != tt.expected.DBDriver {
				t.Errorf("DBDriver = %v, want %v", config.DBDriver, tt.expected.DBDriver)
			}
			if config.DBUser != tt.expected.DBUser {
				t.Errorf("DBUser = %v, want %v", config.DBUser, tt.expected.DBUser)
			}
			if config.DBPass != tt.expected.DBPass {
				t.Errorf("DBPass = %v, want %v", config.DBPass, tt.expected.DBPass)
			}
			if config.DBHost != tt.expected.DBHost {
				t.Errorf("DBHost = %v, want %v", config.DBHost, tt.expected.DBHost)
			}
			if config.DBPort != tt.expected.DBPort {
				t.Errorf("DBPort = %v, want %v", config.DBPort, tt.expected.DBPort)
			}
			if config.DBName != tt.expected.DBName {
				t.Errorf("DBName = %v, want %v", config.DBName, tt.expected.DBName)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "localhost",
				DBPort:   5432,
				DBName:   "testdb",
			},
			wantErr: false,
		},
		{
			name: "invalid port - too high",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "localhost",
				DBPort:   99999,
				DBName:   "testdb",
			},
			wantErr: true,
			errMsg:  "DB_PORT must be a valid port number",
		},
		{
			name: "invalid port - zero",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "localhost",
				DBPort:   0,
				DBName:   "testdb",
			},
			wantErr: true,
			errMsg:  "DB_PORT must be a valid port number",
		},
		{
			name: "empty host",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "",
				DBPort:   5432,
				DBName:   "testdb",
			},
			wantErr: true,
			errMsg:  "DB_HOST cannot be empty",
		},
		{
			name: "whitespace only host",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "   ",
				DBPort:   5432,
				DBName:   "testdb",
			},
			wantErr: true,
			errMsg:  "DB_HOST cannot be empty",
		},
		{
			name: "empty database name",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "localhost",
				DBPort:   5432,
				DBName:   "",
			},
			wantErr: true,
			errMsg:  "DB_NAME cannot be empty",
		},
		{
			name: "empty user",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "",
				DBPass:   "pass",
				DBHost:   "localhost",
				DBPort:   5432,
				DBName:   "testdb",
			},
			wantErr: true,
			errMsg:  "DB_USER cannot be empty",
		},
		{
			name: "empty password - should not error but warn",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "",
				DBHost:   "localhost",
				DBPort:   5432,
				DBName:   "testdb",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("Validate() expected error containing %q, got nil", tt.errMsg)
			}

			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}

			if tt.wantErr && err != nil {
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error = %q, should contain %q", err.Error(), tt.errMsg)
				}
			}
		})
	}
}

func TestGetConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "basic connection string",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "pass",
				DBHost:   "localhost",
				DBPort:   5432,
				DBName:   "testdb",
			},
			expected: "postgres://user:pass@localhost:5432/testdb",
		},
		{
			name: "connection string with special characters in password",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "admin",
				DBPass:   "p@ss:word!",
				DBHost:   "db.example.com",
				DBPort:   5433,
				DBName:   "production",
			},
			expected: "postgres://admin:p%40ss%3Aword%21@db.example.com:5433/production",
		},
		{
			name: "connection string with empty password",
			config: Config{
				DBDriver: "postgres",
				DBUser:   "user",
				DBPass:   "",
				DBHost:   "localhost",
				DBPort:   5432,
				DBName:   "testdb",
			},
			// No ":" before "@": an empty password would override PGPASSWORD and ~/.pgpass
			expected: "postgres://user@localhost:5432/testdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.GetConnectionString()
			if result != tt.expected {
				t.Errorf("GetConnectionString() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestConnectionStringUsesPGPASSWORD checks that without DB_PASS the password comes
// from PGPASSWORD (as the README advises), and that DB_PASS still wins when set.
func TestConnectionStringUsesPGPASSWORD(t *testing.T) {
	t.Setenv("PGPASSWORD", "from-env")
	cfg := Config{DBDriver: "postgres", DBUser: "user", DBHost: "localhost", DBPort: 5432, DBName: "testdb"}

	parsed, err := pgconn.ParseConfig(cfg.GetConnectionString())
	if err != nil {
		t.Fatalf("ParseConfig() error: %v", err)
	}
	if parsed.Password != "from-env" {
		t.Errorf("password = %q, want PGPASSWORD value %q", parsed.Password, "from-env")
	}

	cfg.DBPass = "from-db-pass"
	parsed, err = pgconn.ParseConfig(cfg.GetConnectionString())
	if err != nil {
		t.Fatalf("ParseConfig() error: %v", err)
	}
	if parsed.Password != "from-db-pass" {
		t.Errorf("password = %q, want DB_PASS value %q", parsed.Password, "from-db-pass")
	}
}

func TestGetEnvOrDefault(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		expected     string
	}{
		{
			name:         "returns env value when set",
			key:          "TEST_VAR",
			defaultValue: "default",
			envValue:     "from_env",
			expected:     "from_env",
		},
		{
			name:         "returns default when env not set",
			key:          "TEST_VAR",
			defaultValue: "default",
			envValue:     "",
			expected:     "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			if tt.envValue != "" {
				os.Setenv(tt.key, tt.envValue)
			}

			result := getEnvOrDefault(tt.key, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("getEnvOrDefault() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && stringContains(s, substr)))
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
