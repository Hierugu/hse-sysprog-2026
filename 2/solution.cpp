#include "parser.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

struct exec_result {
    int code = 0;
    bool should_exit = false;
};

struct pipeline_state {
    std::vector<pid_t> process_ids;
    int current_input = STDIN_FILENO;
};

struct parsed_sequence {
    std::vector<std::vector<command>> pipelines;
    std::vector<expr_type> operators;
};

static std::string
get_cd_path(const command& cmd)
{
    if (cmd.args.empty()) {
        const char* home = getenv("HOME");
        return home ? std::string(home) : std::string();
    }
    return cmd.args[0];
}

static int
change_directory(const command& cmd)
{
    std::string path = get_cd_path(cmd);

    if (path.empty()) {
        fprintf(stderr, "cd: HOME not set\n");
        return 1;
    }

    if (chdir(path.c_str()) != 0) {
        fprintf(stderr, "cd: %s: %s\n", path.c_str(), strerror(errno));
        return 1;
    }

    return 0;
}

static int
parse_exit_code(const std::string& arg)
{
    try {
        size_t pos = 0;
        int code = std::stoi(arg, &pos);

        if (pos != arg.length()) {
            return -1;
        }

        if (code < 0 || code > 255) {
            return -1;
        }

        return code;
    } catch (const std::invalid_argument&) {
        return -1;
    } catch (const std::out_of_range&) {
        return -1;
    }
}

static int
get_exit_code(const command& cmd, int last_status)
{
    if (cmd.args.empty()) {
        return last_status;
    }

    int code = parse_exit_code(cmd.args[0]);
    if (code == -1) {
        fprintf(stderr, "exit: invalid exit code: %s\n", cmd.args[0].c_str());
        return 1;
    }

    return code;
}

static std::vector<char*>
make_argv(const command& cmd)
{
    std::vector<char*> argv;
    argv.reserve(cmd.args.size() + 2);
    argv.push_back(const_cast<char*>(cmd.exe.c_str()));
    for (const auto& arg : cmd.args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);
    return argv;
}

static void
close_pipe_descriptors(int pipefd[2], int current_input)
{
    if (pipefd[0] != -1) {
        close(pipefd[0]);
    }
    if (pipefd[1] != -1) {
        close(pipefd[1]);
    }
    if (current_input != STDIN_FILENO) {
        close(current_input);
    }
}

static void
setup_child_redirection(int current_input, int pipefd[2], bool is_last_pipeline,
                        const command_line& line)
{
    if (current_input != STDIN_FILENO) {
        dup2(current_input, STDIN_FILENO);
    }
    if (pipefd[1] != -1) {
        dup2(pipefd[1], STDOUT_FILENO);
    } else if (is_last_pipeline && line.out_type != OUTPUT_TYPE_STDOUT) {
        int flags = O_WRONLY | O_CREAT;
        if (line.out_type == OUTPUT_TYPE_FILE_NEW) {
            flags |= O_TRUNC;
        } else {
            flags |= O_APPEND;
        }
        int fd = open(line.out_file.c_str(), flags, 0666);
        if (fd < 0) {
            perror("open");
            _exit(1);
        }
        dup2(fd, STDOUT_FILENO);
        close(fd);
    }
}

static void
execute_child_command(const command& cmd, int last_status)
{
    if (cmd.exe == "cd") {
        int rc = change_directory(cmd);
        _exit(rc);
    }
    if (cmd.exe == "exit") {
        int code = get_exit_code(cmd, last_status);
        _exit(code);
    }
    std::vector<char*> argv = make_argv(cmd);
    execvp(argv[0], argv.data());
    perror("execvp");
    _exit(127);
}

static int
wait_for_processes(const std::vector<pid_t>& process_ids)
{
    int last_status = 0;
    for (size_t i = 0; i < process_ids.size(); ++i) {
        int status = 0;
        waitpid(process_ids[i], &status, 0);
        if (i + 1 == process_ids.size()) {
            last_status = status;
        }
    }

    if (WIFEXITED(last_status)) {
        return WEXITSTATUS(last_status);
    } else if (WIFSIGNALED(last_status)) {
        return 128 + WTERMSIG(last_status);
    } else {
        return 1;
    }
}

static exec_result
handle_single_builtin(const command& cmd, const command_line& line,
                      bool is_last_pipeline, bool allow_exit, int last_status)
{
    exec_result result{};

    if (cmd.exe == "exit" && allow_exit && line.out_type == OUTPUT_TYPE_STDOUT) {
        result.code = get_exit_code(cmd, last_status);
        result.should_exit = true;
        return result;
    }

    if (cmd.exe == "cd") {
        int saved_stdout = -1;
        int fd = -1;
        if (is_last_pipeline && line.out_type != OUTPUT_TYPE_STDOUT) {
            int flags = O_WRONLY | O_CREAT;
            if (line.out_type == OUTPUT_TYPE_FILE_NEW) {
                flags |= O_TRUNC;
            } else {
                flags |= O_APPEND;
            }
            fd = open(line.out_file.c_str(), flags, 0666);
            if (fd < 0) {
                perror("open");
                result.code = 1;
                return result;
            }
            saved_stdout = dup(STDOUT_FILENO);
            dup2(fd, STDOUT_FILENO);
            close(fd);
        }
        result.code = change_directory(cmd);
        if (saved_stdout != -1) {
            dup2(saved_stdout, STDOUT_FILENO);
            close(saved_stdout);
        }
        return result;
    }

    return result;
}

static exec_result
execute_pipeline(const std::vector<command>& commands, const command_line& line,
                 bool is_last_pipeline, bool allow_exit, int last_status)
{
    exec_result result{};

    if (commands.size() == 1) {
        exec_result builtin_result = handle_single_builtin(commands[0], line,
                                                           is_last_pipeline, allow_exit, last_status);
        if (builtin_result.should_exit || commands[0].exe == "cd") {
            return builtin_result;
        }
    }

    pipeline_state state;

    for (size_t i = 0; i < commands.size(); ++i) {
        int pipefd[2] = {-1, -1};
        if (i + 1 < commands.size()) {
            if (pipe(pipefd) != 0) {
                perror("pipe");
                result.code = 1;
                return result;
            }
        }

        pid_t pid = fork();
        if (pid == 0) {
            setup_child_redirection(state.current_input, pipefd, is_last_pipeline, line);
            close_pipe_descriptors(pipefd, state.current_input);
            execute_child_command(commands[i], last_status);
        }

        if (pid < 0) {
            perror("fork");
            result.code = 1;
            close_pipe_descriptors(pipefd, state.current_input);
            return result;
        }

        state.process_ids.push_back(pid);
        if (state.current_input != STDIN_FILENO) {
            close(state.current_input);
        }
        if (pipefd[1] != -1) {
            close(pipefd[1]);
        }
        state.current_input = pipefd[0];
    }

    if (state.current_input != STDIN_FILENO && state.current_input != -1) {
        close(state.current_input);
    }

    result.code = wait_for_processes(state.process_ids);
    return result;
}

static std::vector<command>
parse_pipeline_commands(std::list<struct expr>::const_iterator& it,
                        const std::list<struct expr>::const_iterator& end)
{
    std::vector<command> pipeline;

    if (it == end || it->type != EXPR_TYPE_COMMAND) {
        return pipeline;
    }

    pipeline.push_back(*it->cmd);
    ++it;

    for (; it != end && it->type == EXPR_TYPE_PIPE; ++it) {
        if (++it == end || it->type != EXPR_TYPE_COMMAND) {
            break;
        }
        pipeline.push_back(*it->cmd);
    }

    return pipeline;
}

static parsed_sequence
parse_command_sequence(const command_line* line)
{
    parsed_sequence result;
    auto it = line->exprs.begin();
    const auto end = line->exprs.end();

    for (; it != end;) {
        std::vector<command> pipeline = parse_pipeline_commands(it, end);
        if (!pipeline.empty()) {
            result.pipelines.push_back(std::move(pipeline));
        }

        if (it != end && (it->type == EXPR_TYPE_AND || it->type == EXPR_TYPE_OR)) {
            result.operators.push_back(it->type);
            ++it;
        }
    }

    return result;
}

static void
cleanup_background(std::vector<pid_t>& background)
{
    auto new_end = std::remove_if(background.begin(), background.end(),
                                  [](pid_t pid) {
                                      int st = 0;
                                      pid_t res = waitpid(pid, &st, WNOHANG);
                                      return res != 0;
                                  });
    background.erase(new_end, background.end());
}

static bool
run_command_sequence(const command_line* line, int& last_status, bool allow_exit)
{
    parsed_sequence parsed = parse_command_sequence(line);

    auto should_execute = [&](size_t pipeline_index, int current_status) -> bool {
        if (pipeline_index == 0) {
            return true;
        }
        expr_type op = parsed.operators[pipeline_index - 1];
        return (op == EXPR_TYPE_AND && current_status == 0) ||
               (op == EXPR_TYPE_OR && current_status != 0);
    };

    int current_status = last_status;
    auto pipeline_it = parsed.pipelines.begin();
    auto op_it = parsed.operators.begin();

    for (size_t i = 0; pipeline_it != parsed.pipelines.end(); ++i, ++pipeline_it) {
        if (!should_execute(i, current_status)) {
            continue;
        }

        bool is_last = (std::next(pipeline_it) == parsed.pipelines.end());
        exec_result res = execute_pipeline(*pipeline_it, *line, is_last, allow_exit, current_status);
        current_status = res.code;

        if (res.should_exit) {
            last_status = current_status;
            return true;
        }

        if (op_it != parsed.operators.end()) {
            ++op_it;
        }
    }

    last_status = current_status;
    return false;
}

static bool
execute_background_command(const struct command_line* line, int last_status,
                           std::vector<pid_t>& bg_processes)
{
    pid_t pid = fork();
    if (pid == 0) {
        int child_status = last_status;
        run_command_sequence(line, child_status, false);
        _exit(child_status);
    }
    if (pid < 0) {
        perror("fork");
        return false;
    }
    bg_processes.push_back(pid);
    return true;
}

static bool
process_command_line(const struct command_line* line, int& last_status,
                     std::vector<pid_t>& bg_processes)
{
    if (line->is_background) {
        bool success = execute_background_command(line, last_status, bg_processes);
        if (success) {
            last_status = 0;
        } else {
            last_status = 1;
        }
        return false;
    }
    return run_command_sequence(line, last_status, true);
}

static int
run_shell_loop()
{
    const size_t buf_size = 1024;
    char buf[buf_size];
    int bytes_read;
    struct parser* p = parser_new();
    int last_status = 0;
    std::vector<pid_t> bg_processes;

    while ((bytes_read = read(STDIN_FILENO, buf, buf_size)) > 0) {
        parser_feed(p, buf, bytes_read);

        struct command_line* line = NULL;
        while (true) {
            enum parser_error err = parser_pop_next(p, &line);
            if (err == PARSER_ERR_NONE && line == NULL) {
                break;
            }
            if (err != PARSER_ERR_NONE) {
                printf("Error: %d\n", (int)err);
                continue;
            }

            bool should_exit = process_command_line(line, last_status, bg_processes);
            delete line;
            cleanup_background(bg_processes);
            if (should_exit) {
                parser_delete(p);
                return last_status;
            }
        }
        cleanup_background(bg_processes);
    }

    parser_delete(p);
    cleanup_background(bg_processes);
    return last_status;
}

int main(void)
{
    return run_shell_loop();
}
