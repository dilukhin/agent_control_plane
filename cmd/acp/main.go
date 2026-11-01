// acp is a finite local project-check pilot, not a daemon or general agent shell.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/localcheck"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

const version = "0.1.0-pilot"

var sourceCommit = "development"

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && args[0] == "--version" {
		fmt.Fprintln(stdout, "acp", version, sourceCommit)
		return 0
	}
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, "ACP: конечная местная проверка проекта")
		fmt.Fprintln(stdout, "Команды: prepare, check, status, recover, gc. Для справки: acp check --help")
		return 0
	}
	command := args[0]
	if command != "prepare" && command != "check" && command != "status" && command != "recover" && command != "gc" {
		fmt.Fprintln(stderr, "Неизвестная команда")
		return 2
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	root := f.String("state", "", "Отдельный приватный каталог состояния ACP")
	id := f.String("task", "", "Устойчивый ID задания, 1–64 латинских букв/цифр, дефис или подчёркивание")
	var spec, program, workspace *string
	var show *bool
	var after, receipt *string
	var profileOutput *string
	if command == "check" || command == "prepare" {
		spec = f.String("spec", "", "Файл доверенного профиля проверки JSON v1")
		program = f.String("program", "", "Выбранная программа или её абсолютный путь")
		workspace = f.String("workspace", "", "Каталог проверяемого проекта")
		show = f.Bool("show-output", false, "Показать местный вывод программы; по умолчанию он не выводится и не сохраняется")
	}
	if command == "prepare" {
		profileOutput = f.String("out", "", "Создать новый профиль только для точных исходников или их CRLF/LF-эквивалента")
	}
	if command == "gc" {
		after = f.String("after", "", "Продолжить страницу очистки с state.Next предыдущего ответа")
		receipt = f.String("receipt", "", "Удалить одну квитанцию старше 30 суток, если её Task уже удалена")
	}
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if command == "prepare" {
		if f.NArg() != 0 || *spec == "" || *program == "" || *workspace == "" || *profileOutput == "" {
			fmt.Fprintln(stderr, "Для prepare требуются --spec, --program, --workspace и --out")
			return 2
		}
		path, err := exec.LookPath(*program)
		if err != nil || localcheck.Prepare(*spec, path, *workspace, *profileOutput) != nil {
			fmt.Fprintln(stderr, "Профиль не создан: проверьте исходники, программу и отсутствие выходного файла; разрешено только отличие CRLF/LF")
			return 2
		}
		fmt.Fprintln(stdout, "Создан местный профиль; код проекта не изменялся")
		return 0
	}
	if f.NArg() != 0 || *root == "" || (command != "gc" && *id == "") {
		fmt.Fprintln(stderr, "Требуются --state и --task; у gc только --state")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 310*time.Second)
	defer cancel()
	var result localcheck.Result
	var err error
	var output []byte
	switch command {
	case "check":
		if *spec == "" || *program == "" || *workspace == "" {
			fmt.Fprintln(stderr, "Для check требуются --spec, --program и --workspace")
			return 2
		}
		path, e := exec.LookPath(*program)
		if e != nil {
			fmt.Fprintln(stderr, "Выбранная программа недоступна")
			return 2
		}
		c, e := localcheck.Load(*spec, path, *workspace)
		if e != nil {
			fmt.Fprintln(stderr, "Профиль, программа или снимок исходников не прошли проверку")
			return 2
		}
		result, output, err = localcheck.Check(ctx, *root, *id, c)
	case "status":
		result, err = localcheck.Status(ctx, *root, *id)
	case "recover":
		result, err = localcheck.Recover(ctx, *root, *id)
	case "gc":
		n, e := localcheck.Collect(ctx, *root, *after, *receipt)
		if e != nil {
			fmt.Fprintln(stderr, "Очистка не завершена; сохранённое состояние требует проверки")
			return 2
		}
		json.NewEncoder(stdout).Encode(n)
		return 0
	}
	if result.Task != "" {
		json.NewEncoder(stdout).Encode(result)
	}
	if err != nil {
		switch {
		case errors.Is(err, localcheck.ErrExisting):
			fmt.Fprintln(stderr, localcheck.ErrExisting)
		case errors.Is(err, localcheck.ErrReceipt):
			fmt.Fprintln(stderr, localcheck.ErrReceipt)
		default:
			// Do not print raw process, filesystem or SQLite diagnostics.
			fmt.Fprintln(stderr, "Операция не подтверждена. Прочитайте status; повторное исполнение не выполняется")
		}
		return 2
	}
	if show != nil && *show {
		stderr.Write(output)
	}
	if result.Operation == state.OperationFailed {
		return 1
	}
	if result.Operation != state.OperationSucceeded {
		return 2
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
