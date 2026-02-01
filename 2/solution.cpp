#include "parser.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <vector>
#include <signal.h>
#include <optional>

static std::vector<pid_t> background_jobs;

static bool execute_command_line(const struct command_line *line, int &exit_code);

static void cleanup_zombies() {
	for (auto it = background_jobs.begin(); it != background_jobs.end(); ) {
		int status;
		pid_t result = waitpid(*it, &status, WNOHANG);
		if (result > 0) {
			it = background_jobs.erase(it);
		} else if (result == -1 && errno == ECHILD) {
			it = background_jobs.erase(it);
		} else {
			++it;
		}
	}
}

static int
execute_cd_command(const struct command *cmd)
{
	if (cmd->args.empty()) {
		const char *home = getenv("HOME");
		if (home == nullptr) {
			fprintf(stderr, "cd: HOME not set\n");
			return 1;
		}
		if (chdir(home) != 0) {
			perror("cd");
			return 1;
		}
	} else {
		if (chdir(cmd->args[0].c_str()) != 0) {
			perror("cd");
			return 1;
		}
	}
	return 0;
}

static bool
execute_logical_expression(const std::list<expr> &exprs, const struct command_line *line, int &exit_code);

static bool
execute_pipeline(const std::list<expr> &exprs, const struct command_line *line, int &exit_code) {
	std::vector<struct command> commands;
	for (const auto &e : exprs) {
		if (e.type == EXPR_TYPE_COMMAND) {
			commands.push_back(e.cmd.value());
		}
	}
	
	if (commands.empty()) {
		return false;
	}
	
	if (commands.size() == 1) {
		const struct command *cmd = &commands[0];
		
		if (cmd->exe == "cd") {
			exit_code = execute_cd_command(cmd);
			return false;
		}
		
		if (cmd->exe == "exit") {
			if (line->out_type == OUTPUT_TYPE_STDOUT) {
				exit_code = 0;
				if (!cmd->args.empty()) {
					exit_code = atoi(cmd->args[0].c_str());
				}
				return true;
			}
			return false;
		}
		
		pid_t pid = fork();
		if (pid == 0) {
			if (line->is_background) {
				signal(SIGTTIN, SIG_IGN);
				signal(SIGTTOU, SIG_IGN);
			}
			
			if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
				int fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
				if (fd < 0) {
					perror("open");
					exit(1);
				}
				dup2(fd, STDOUT_FILENO);
				close(fd);
			} else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
				int fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
				if (fd < 0) {
					perror("open");
					exit(1);
				}
				dup2(fd, STDOUT_FILENO);
				close(fd);
			}
			
			char **argv = new char*[cmd->args.size() + 2];
			argv[0] = strdup(cmd->exe.c_str());
			for (size_t i = 0; i < cmd->args.size(); ++i) {
				argv[i + 1] = strdup(cmd->args[i].c_str());
			}
			argv[cmd->args.size() + 1] = nullptr;
			
			execvp(cmd->exe.c_str(), argv);
			perror("execvp");
			
			for (size_t i = 0; i <= cmd->args.size(); ++i) {
				free(argv[i]);
			}
			delete[] argv;
			exit(EXIT_FAILURE);
		} else if (pid > 0) {
			if (line->is_background) {
				background_jobs.push_back(pid);
				exit_code = 0;
			} else {
				int status;
				waitpid(pid, &status, 0);
				exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
			}
		} else {
			perror("fork");
			exit_code = 1;
		}
		
		return false;
	}
	
	std::vector<int> pipe_fds;
	
	for (size_t i = 0; i < commands.size() - 1; ++i) {
		int pipefd[2];
		if (pipe(pipefd) == -1) {
			perror("pipe");
			for (auto &fd : pipe_fds) {
				close(fd);
			}
			exit_code = 1;
			return false;
		}
		pipe_fds.push_back(pipefd[0]);
		pipe_fds.push_back(pipefd[1]);
	}
	
	std::vector<pid_t> pids;
	bool is_last_cmd_with_file_redirect = false;
	
	for (size_t i = 0; i < commands.size(); ++i) {
		int input_fd = (i == 0) ? STDIN_FILENO : pipe_fds[(i - 1) * 2];
		int output_fd = STDOUT_FILENO;
		
		if (i < commands.size() - 1) {
			output_fd = pipe_fds[i * 2 + 1];
		}
		is_last_cmd_with_file_redirect = (i == commands.size() - 1) &&
			(line->out_type == OUTPUT_TYPE_FILE_NEW || line->out_type == OUTPUT_TYPE_FILE_APPEND);
		
		
		if (commands[i].exe == "cd") {
			if (i == 0) {
				exit_code = execute_cd_command(&commands[i]);
			}
			for (auto &fd : pipe_fds) {
				close(fd);
			}
			return false;
		}
		
		if (commands[i].exe == "exit") {
			int exit_code_local = 0;
			if (!commands[i].args.empty()) {
				exit_code_local = atoi(commands[i].args[0].c_str());
			}
			
			pid_t pid = fork();
			if (pid == 0) {
				if (input_fd != STDIN_FILENO) {
					dup2(input_fd, STDIN_FILENO);
					close(input_fd);
				}
				if (output_fd != STDOUT_FILENO) {
					dup2(output_fd, STDOUT_FILENO);
					close(output_fd);
				}
				
				for (auto &fd : pipe_fds) {
					close(fd);
				}
				
				exit(exit_code_local);
			} else if (pid > 0) {
				pids.push_back(pid);
			} else {
				perror("fork");
				for (auto &fd : pipe_fds) {
					close(fd);
				}
				exit_code = 1;
				return false;
			}
			continue;
		}
		
		pid_t pid = fork();
		if (pid == 0) {
			if (line->is_background) {
				signal(SIGTTIN, SIG_IGN);
				signal(SIGTTOU, SIG_IGN);
			}
			
			if (input_fd != STDIN_FILENO) {
				dup2(input_fd, STDIN_FILENO);
				close(input_fd);
			}
			
			if (is_last_cmd_with_file_redirect) {
				int fd;
				if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
					fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
				} else {
					fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
				}
				if (fd < 0) {
					perror("open");
					exit(1);
				}
				dup2(fd, STDOUT_FILENO);
				close(fd);
			} else if (output_fd != STDOUT_FILENO) {
				dup2(output_fd, STDOUT_FILENO);
				close(output_fd);
			}
			
			for (auto &fd : pipe_fds) {
				close(fd);
			}
			
			char **argv = new char*[commands[i].args.size() + 2];
			argv[0] = strdup(commands[i].exe.c_str());
			for (size_t j = 0; j < commands[i].args.size(); ++j) {
				argv[j + 1] = strdup(commands[i].args[j].c_str());
			}
			argv[commands[i].args.size() + 1] = nullptr;
			
			execvp(commands[i].exe.c_str(), argv);
			
			perror("execvp");
			
			for (size_t j = 0; j <= commands[i].args.size(); ++j) {
				free(argv[j]);
			}
			delete[] argv;
			
			exit(EXIT_FAILURE);
		} else if (pid > 0) {
			pids.push_back(pid);
		} else {
			perror("fork");
			for (auto &fd : pipe_fds) {
				close(fd);
			}
			exit_code = 1;
			return false;
		}
	}
	
	for (auto &fd : pipe_fds) {
		close(fd);
	}
	
	if (!line->is_background) {
		int last_status = 0;
		for (auto pid : pids) {
			int status;
			waitpid(pid, &status, 0);
			last_status = status;
		}
		
		if (WIFEXITED(last_status)) {
			exit_code = WEXITSTATUS(last_status);
		}
	} else {
		for (auto pid : pids) {
			background_jobs.push_back(pid);
		}
	}
	
	if (commands.size() == 1 && commands[0].exe == "exit" &&
		line->out_type == OUTPUT_TYPE_STDOUT) {
		exit_code = 0;
		if (!commands[0].args.empty()) {
			exit_code = atoi(commands[0].args[0].c_str());
		}
		return true;
	}
	
	return false;
}

static bool
execute_logical_expression(const std::list<expr> &exprs, const struct command_line *line, int &exit_code) {
	std::vector<std::list<expr>> segments;
	std::vector<expr_type> operators;
	
	std::list<expr> current_segment;
	for (const auto &e : exprs) {
		if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {
			if (!current_segment.empty()) {
				segments.push_back(current_segment);
				operators.push_back(e.type);
				current_segment.clear();
			}
		} else {
			current_segment.push_back(e);
		}
	}
	if (!current_segment.empty()) {
		segments.push_back(current_segment);
	}
	
	if (segments.empty()) {
		return false;
	}
	
	exit_code = 0;
	
	for (size_t i = 0; i < segments.size(); ++i) {
		if (i > 0) {
			expr_type prev_op = operators[i - 1];
			if (prev_op == EXPR_TYPE_AND && exit_code != 0) {
				continue;
			}
			if (prev_op == EXPR_TYPE_OR && exit_code == 0) {
				continue;
			}
		}
		
		command_line temp_line = *line;
		temp_line.exprs = segments[i];
		bool should_exit = execute_pipeline(temp_line.exprs, &temp_line, exit_code);
		
		if (should_exit) {
			return true;
		}
	}
	
	return false;
}

static bool
execute_command_line(const struct command_line *line, int &exit_code)
{
	assert(line != NULL);
	
	if (line->exprs.empty()) {
		return false;
	}
	
	bool has_logical_operators = false;
	for (const auto &e : line->exprs) {
		if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {
			has_logical_operators = true;
			break;
		}
	}
	
	if (has_logical_operators) {
		return execute_logical_expression(line->exprs, line, exit_code);
	}
	
	return execute_pipeline(line->exprs, line, exit_code);
}

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	int exit_code = 0;
	bool should_exit = false;
	struct parser *p = parser_new();
	
	while (!should_exit && (rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				fprintf(stderr, "Parser error: %d\n", (int)err);
				continue;
			}
			should_exit = execute_command_line(line, exit_code);
			delete line;
			cleanup_zombies();
		}
	}
	
	while (!background_jobs.empty()) {
		cleanup_zombies();
		if (!background_jobs.empty()) {
			usleep(10000);
		}
	}
	
	parser_delete(p);
	return exit_code;
}
