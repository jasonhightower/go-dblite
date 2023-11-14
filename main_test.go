package main

import (
    "os"
    "fmt"
	"bytes"
	"io"
	"strings"
	"testing"
	"github.com/stretchr/testify/require"
)

const (
    TEST_DB_FILE = "test.db"
)

func setupTest() {
    if err := os.Remove(TEST_DB_FILE); err != nil && !os.IsNotExist(err) {
        panic(err)
    }
}

func runScript(commands []string) []string {
    var input io.Reader = strings.NewReader(strings.Join(commands, "\n") + "\n")
    outputBuffer := new(bytes.Buffer)
    var output io.Writer = outputBuffer

    exec(TEST_DB_FILE, &input, &output)

    actualOutput := strings.Split(outputBuffer.String(), "\n")
    actualOutput = actualOutput[:len(actualOutput)-1]
    return actualOutput

}

func TestInsertAndRetrieve(t *testing.T) {
    setupTest()
    result := runScript(
        []string {
            "insert 1 jasonh jasonh@example.test",
            "select",
            ".exit",
        }) 
    require.Equal(
        t, 
        []string {
            "dblite > Executing",
            "dblite > Executing",
            " 1 | jasonh | jasonh@example.test",
            "dblite > Exiting.",
        },
        result)
}

func TestErrorWhenTableIsFull(t *testing.T) {
    setupTest()
    script := make([]string, TABLE_MAX_ROWS + 2)
    for i := 0; i < len(script) - 1; i++ {
        script[i] = fmt.Sprintf("insert %d user%d user%d@gmail.com", i, i, i)
    }
    script[len(script) - 1] = ".exit"
    result := runScript(script)

    require.Equal(t, "Error: Table full.", result[len(result) - 2])
}

func TestMaxLengthString(t *testing.T) {
    setupTest()
    username := strings.Repeat("a", COLUMN_USERNAME_SIZE)
    email := strings.Repeat("a", COLUMN_EMAIL_SIZE)
    result := runScript(
        []string {
            fmt.Sprintf("insert 1 %s jasonh@example.test", username),
            fmt.Sprintf("insert 2 jasonh %s", email),
            ".exit",
        })
    require.Equal(
        t,
        []string {
            "dblite > Executing",
            "dblite > Executing",
            "dblite > Exiting.",
        },
        result)
}

func TestStringLength(t *testing.T) {
    setupTest()
    username := strings.Repeat("a", COLUMN_USERNAME_SIZE + 1)
    email := strings.Repeat("a", COLUMN_EMAIL_SIZE + 1)
    result := runScript(
        []string {
            fmt.Sprintf("insert 1 %s jasonh@example.test", username),
            fmt.Sprintf("insert 2 jasonh %s", email),
            ".exit",
        })
    require.Equal(
        t,
        []string {
            "dblite > username is too long",
            "dblite > email is too long",
            "dblite > Exiting.",
        },
        result)
}

func TestNegativeId(t *testing.T) {
    setupTest()
    result := runScript(
        []string {
            "insert -1 jasonh jasonh@example.test",
            ".exit",
        }) 
    require.Equal(
        t, 
        []string {
            "dblite > ID must be positive.", 
            "dblite > Exiting.",
        },
        result)
}

func TestDataIsPersisted(t *testing.T) {
    setupTest()
    result := runScript(
        []string {
            "insert 1 jasonh jasonh@example.test",
            ".exit",
        })
    require.Equal(
        t,
        []string {
            "dblite > Executing",
            "dblite > Exiting.",
        },
        result)
    result = runScript(
        []string {
            "select",
            ".exit",
        })
    require.Equal(
        t,
        []string {
            "dblite > Executing",
            " 1 | jasonh | jasonh@example.test",
            "dblite > Exiting.",
        },
        result)
} 
