/**
 * @file qconf.c
 * @brief Slurm configuration query tool mimicking SGE qconf.
 *
 * Implements a command-line interface for querying Slurm configuration details,
 * supporting SGE-compatible options to list queues (-sql, -sq), parallel environments
 * (-spl, -sp), and display version information (-version). Currently, only -sql and
 * -version are fully implemented.
 *
 * @author HE JIALE
 * @date 2025-04-04
 * @version 0.0.1
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include "qconf.h"

/**
 * @brief List all Slurm partitions (queues).
 * @param param Unused parameter.
 * @return 0 on success, 1 on error.
 */
static int list_all_queues(const char *param) {
    (void)param; /* unused param */
    partition_info_msg_t *part_info = NULL;

    if (slurm_load_partitions(0, &part_info, SHOW_ALL) != SLURM_SUCCESS) {
        fprintf(stderr, "Failed to load partitions: %s\n", slurm_strerror(slurm_get_errno()));
        return 1;
    }

    for (uint32_t i = 0; i < part_info->record_count; i++) {
        printf("%s\n", part_info->partition_array[i].name);
    }

    slurm_free_partition_info_msg(part_info);
    return 0;
}

/**
 * @brief List details for a specific Slurm partition (queue).
 * @param param Partition name.
 * @return 0 on success, 1 on error.
 * @note Currently unimplemented.
 */
static int list_specific_queue(const char *param) {
    if (!param || !*param) {
        fprintf(stderr, "Queue name required for -sq\n");
        return 1;
    }
    /* TODO: Implement specific queue listing */
    fprintf(stderr, "Listing specific queue (%s) not implemented\n", param);
    return 0;
}

/**
 * @brief List all parallel environments.
 * @param param Unused parameter.
 * @return 0 on success, 1 on error.
 * @note Currently unimplemented.
 */
static int list_all_parallel_envs(const char *param) {
     (void)param;
    /* TODO: Implement parallel environment listing */
    fprintf(stderr, "Listing all parallel environments not implemented\n");
    return 0;
}

/**
 * @brief List details for a specific parallel environment.
 * @param param Parallel environment name.
 * @return 0 on success, 1 on error.
 * @note Currently unimplemented.
 */
static int list_specific_parallel_env(const char *param) {
    if (!param || !*param) {
        fprintf(stderr, "Parallel environment name required for -sp\n");
        return 1;
    }
    /* TODO: Implement specific parallel environment listing */
    fprintf(stderr, "Listing specific parallel environment (%s) not implemented\n", param);
    return 0;
}

/**
 * @brief Display version information for qconf.
 * @param param Unused parameter.
 * @return 0 on success.
 */
static int display_version(const char *param) {
    (void)param;
    printf("METASTACK SGE WRAPPER\n");
    printf("Version: %s\n", PROGRAM_VERSION);
    printf("Release Date: %s\n", RELEASE_DATE);
    printf("Supported options: -sql, -sq, -spl, -sp, -version\n");
    return 0;
}

/**
 * @brief Add a command handler to the parser.
 * @param parser Command parser.
 * @param handler Command handler to add.
 */
static void add_handler(CommandParser *parser, CommandHandler handler) {
    if (!parser)
        return;
    parser->handlers = xrealloc(parser->handlers,
                               sizeof(CommandHandler) * (parser->handler_count + 1));
    parser->handlers[parser->handler_count++] = handler;
}

/**
 * @brief Free the command parser's resources.
 * @param parser Command parser to free.
 */
static void destroy_parser(CommandParser *parser) {
    if (!parser) return;
    xfree(parser->handlers);
    xfree(parser);
}

/**
 * @brief Parse command-line arguments.
 * @param parser Command parser.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
static int parse_command(CommandParser *parser, int argc, char *argv[]) {
    if (!parser || !argv) {
        fprintf(stderr, "Invalid parser or arguments\n");
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        bool command_found = false;
        for (int j = 0; j < parser->handler_count; j++) {
            if (strcmp(argv[i], parser->handlers[j].command)) continue;

            command_found = true;
            switch (parser->handlers[j].require_param) {
                case OPTION_TYPE_NONE:
                    if (parser->handlers[j].execute(NULL)) return 1;
                    break;
                case OPTION_TYPE_REQUIRED:
                    if (i + 1 >= argc || argv[i + 1][0] == '-') {
                        fprintf(stderr, "Command %s requires a parameter\n", argv[i]);
                        return 1;
                    }
                    if (parser->handlers[j].execute(argv[++i])) return 1;
                    break;
                default:
                    fprintf(stderr, "Unsupported option type for %s\n", argv[i]);
                    return 1;
            }
            break;
        }

        if (!command_found) {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }
    return 0;
}

/**
 * @brief Create a command parser.
 * @return Allocated CommandParser, caller must free with destroy_parser.
 */
static CommandParser *create_command_parser(void) {
    CommandParser *parser = xmalloc(sizeof(CommandParser));
    parser->handlers = NULL;
    parser->handler_count = 0;
    parser->add_handler = add_handler;
    parser->parse = parse_command;
    parser->destroy = destroy_parser;

    CommandHandler commands[] = {
        {"-sql", OPTION_TYPE_NONE, list_all_queues},
        {"-sq", OPTION_TYPE_REQUIRED, list_specific_queue},
        {"-spl", OPTION_TYPE_NONE, list_all_parallel_envs},
        {"-sp", OPTION_TYPE_REQUIRED, list_specific_parallel_env},
        {"-version", OPTION_TYPE_NONE, display_version}
    };

    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        parser->add_handler(parser, commands[i]);
    }

    return parser;
}

/**
 * @brief Main entry point for qconf.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s [options]\n", argv[0]);
        return 1;
    }
    slurm_init(NULL);
    CommandParser *parser = create_command_parser();
    int rc = parser->parse(parser, argc, argv);
    parser->destroy(parser);
    return rc;
}