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

static int
execute_single_command(const struct command *cmd, int input_fd, int output_fd)
{
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
		if (input_fd != STDIN_FILENO) {
			close(input_fd);
		}
		if (output_fd != STDOUT_FILENO) {
			close(output_fd);
		}
		
		int status;
		waitpid(pid, &status, 0);
		return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
	} else {
		perror("fork");
		return 1;
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
execute_command_line(const struct command_line *line, int &exit_code)
{
	assert(line != NULL);
	
	if (line->exprs.empty()) {
		return false;
	}
	
	if (line->exprs.size() == 1 && line->exprs.front().type == EXPR_TYPE_COMMAND) {
		const struct command *cmd = &line->exprs.front().cmd.value();
		
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
		
		int output_fd = STDOUT_FILENO;
		if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
			output_fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
			if (output_fd < 0) {
				perror("open");
				return false;
			}
		} else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
			output_fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
			if (output_fd < 0) {
				perror("open");
				return false;
			}
		}
		
		int result = execute_single_command(cmd, STDIN_FILENO, output_fd);
		exit_code = result;
		
		if (output_fd != STDOUT_FILENO) {
			close(output_fd);
		}
		return false;
	}
	std::vector<struct command> commands;
	std::vector<int> pipe_fds;
	
	for (const auto &e : line->exprs) {
		if (e.type == EXPR_TYPE_COMMAND) {
			commands.push_back(e.cmd.value());
		}
	}
	
	if (commands.empty()) {
		return false;
	}
	
	for (size_t i = 0; i < commands.size() - 1; ++i) {
		int pipefd[2];
		if (pipe(pipefd) == -1) {
			perror("pipe");
			for (auto &fd : pipe_fds) {
				close(fd);
			}
			return false;
		}
		pipe_fds.push_back(pipefd[0]);
		pipe_fds.push_back(pipefd[1]);
	}
	std::vector<pid_t> pids;
	for (size_t i = 0; i < commands.size(); ++i) {
		int input_fd = (i == 0) ? STDIN_FILENO : pipe_fds[(i - 1) * 2];
		int output_fd = STDOUT_FILENO;
		
		if (i < commands.size() - 1) {
			output_fd = pipe_fds[i * 2 + 1];
		}
		
		if (i == commands.size() - 1) {
			if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
				output_fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
				if (output_fd < 0) {
					perror("open");
					for (auto &fd : pipe_fds) {
						close(fd);
					}
					return false;
				}
			} else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
				output_fd = open(line->out_file.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
				if (output_fd < 0) {
					perror("open");
					for (auto &fd : pipe_fds) {
						close(fd);
					}
					return false;
				}
			}
		}
		
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
				return false;
			}
			continue;
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
			return false;
		}
	}
	
	for (auto &fd : pipe_fds) {
		close(fd);
	}
	
	int last_status = 0;
	for (auto pid : pids) {
		int status;
		waitpid(pid, &status, 0);
		last_status = status;
	}
	
	if (WIFEXITED(last_status)) {
		exit_code = WEXITSTATUS(last_status);
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
		}
	}
	
	parser_delete(p);
	return exit_code;
}
