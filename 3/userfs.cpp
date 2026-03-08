#include "userfs.h"

#include "rlist.h"

#include <stddef.h>
#include <string>
#include <vector>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char memory[BLOCK_SIZE];
	/** A link in the block list of the owner-file. */
	rlist in_block_list = RLIST_LINK_INITIALIZER;

	/* PUT HERE OTHER MEMBERS */
};

struct file {
	/**
	 * Doubly-linked intrusive list of file blocks. Intrusiveness of the
	 * list gives you the full control over the lifetime of the items in the
	 * list without having to use double pointers with performance penalty.
	 */
	rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
	/** How many file descriptors are opened on the file. */
	int refs = 0;
	/** File name. */
	std::string name;
	/** A link in the global file list. */
	rlist in_file_list = RLIST_LINK_INITIALIZER;
	/** Current file size in bytes. */
	size_t size = 0;

	/* PUT HERE OTHER MEMBERS */
};

/**
 * Intrusive list of all files. In this case the intrusiveness of the list also
 * grants the ability to remove items from any position in O(1) complexity
 * without having to know their iterator.
 */
static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
	file *atfile;
	block *current_block;
	size_t offset;
	size_t pos;
#if NEED_OPEN_FLAGS
	int flags;
#endif
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static std::vector<filedesc*> file_descriptors;

static file *
find_file_by_name(const char *filename)
{
	file *f;
	rlist_foreach_entry(f, &file_list, in_file_list) {
		if (f->name == filename)
			return f;
	}
	return NULL;
}

static filedesc *
find_filedesc_by_fd(int fd)
{
	if (fd < 0 || fd >= (int)file_descriptors.size())
		return NULL;
	return file_descriptors[fd];
}

static block *
allocate_block()
{
	block *b = new block();
	rlist_create(&b->in_block_list);
	return b;
}

static void
free_file_blocks(file *f)
{
	block *b, *tmp;
	rlist_foreach_entry_safe(b, &f->blocks, in_block_list, tmp) {
		rlist_del_entry(b, in_block_list);
		delete b;
	}
}

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

int
ufs_open(const char *filename, int flags)
{
	file *f = find_file_by_name(filename);
	
	if (!f) {
		if (!(flags & UFS_CREATE)) {
			ufs_error_code = UFS_ERR_NO_FILE;
			return -1;
		}
		f = new file();
		f->name = filename;
		rlist_add_tail_entry(&file_list, f, in_file_list);
	}
	
	filedesc *desc = new filedesc();
	desc->atfile = f;
	desc->offset = 0;
	desc->pos = 0;
	if (rlist_empty(&f->blocks)) {
		desc->current_block = NULL;
	} else {
		desc->current_block = rlist_first_entry(&f->blocks, block, in_block_list);
	}
#if NEED_OPEN_FLAGS
	if ((flags & UFS_READ_WRITE) == UFS_READ_WRITE) {
		desc->flags = UFS_READ_WRITE;
	} else if (flags & UFS_READ_ONLY) {
		desc->flags = UFS_READ_ONLY;
	} else if (flags & UFS_WRITE_ONLY) {
		desc->flags = UFS_WRITE_ONLY;
	} else {
		desc->flags = UFS_READ_WRITE;
	}
#endif
	f->refs++;
	
	int fd = -1;
	for (size_t i = 0; i < file_descriptors.size(); ++i) {
		if (!file_descriptors[i]) {
			fd = i;
			break;
		}
	}
	if (fd == -1) {
		fd = file_descriptors.size();
		file_descriptors.push_back(desc);
	} else {
		file_descriptors[fd] = desc;
	}
	
	ufs_error_code = UFS_ERR_NO_ERR;
	return fd;
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	filedesc *desc = find_filedesc_by_fd(fd);
	if (!desc) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
#if NEED_OPEN_FLAGS
	if (desc->flags == UFS_READ_ONLY) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	
	if (desc->pos + size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	
	file *f = desc->atfile;
	size_t written = 0;
	size_t remaining = size;
	
	while (remaining > 0) {
		if (!desc->current_block) {
			block *b = allocate_block();
			rlist_add_tail_entry(&f->blocks, b, in_block_list);
			desc->current_block = b;
			desc->offset = 0;
		}
		
		size_t to_write = remaining;
		if (to_write > BLOCK_SIZE - desc->offset)
			to_write = BLOCK_SIZE - desc->offset;
		
		memcpy(desc->current_block->memory + desc->offset, buf + written, to_write);
		
		desc->offset += to_write;
		written += to_write;
		remaining -= to_write;
		desc->pos += to_write;
		
		if (desc->offset == BLOCK_SIZE) {
			rlist *next = desc->current_block->in_block_list.next;
			if (next == &f->blocks) {
				desc->current_block = NULL;
			} else {
				desc->current_block = rlist_entry(next, block, in_block_list);
			}
			desc->offset = 0;
		}
	}
	
	if (desc->pos > f->size)
		f->size = desc->pos;
	
	ufs_error_code = UFS_ERR_NO_ERR;
	return written;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	filedesc *desc = find_filedesc_by_fd(fd);
	if (!desc) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
#if NEED_OPEN_FLAGS
	if (desc->flags == UFS_WRITE_ONLY) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	
	if (desc->pos >= desc->atfile->size) {
		ufs_error_code = UFS_ERR_NO_ERR;
		return 0;
	}
	
	if (!desc->current_block) {
		if (rlist_empty(&desc->atfile->blocks)) {
			ufs_error_code = UFS_ERR_NO_ERR;
			return 0;
		}
		desc->current_block = rlist_first_entry(&desc->atfile->blocks, block, in_block_list);
		desc->offset = 0;
	}
	
	size_t read_bytes = 0;
	size_t remaining = size;
	
	while (remaining > 0 && desc->current_block && desc->pos < desc->atfile->size) {
		size_t to_read = remaining;
		if (to_read > BLOCK_SIZE - desc->offset)
			to_read = BLOCK_SIZE - desc->offset;
		if (to_read > desc->atfile->size - desc->pos)
			to_read = desc->atfile->size - desc->pos;
		
		memcpy(buf + read_bytes, desc->current_block->memory + desc->offset, to_read);
		
		desc->offset += to_read;
		read_bytes += to_read;
		remaining -= to_read;
		desc->pos += to_read;
		
		if (desc->offset == BLOCK_SIZE) {
			rlist *next = desc->current_block->in_block_list.next;
			if (next == &desc->atfile->blocks) {
				desc->current_block = NULL;
			} else {
				desc->current_block = rlist_entry(next, block, in_block_list);
			}
			desc->offset = 0;
		}
	}
	
	ufs_error_code = UFS_ERR_NO_ERR;
	return read_bytes;
}

int
ufs_close(int fd)
{
	filedesc *desc = find_filedesc_by_fd(fd);
	if (!desc) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	file *f = desc->atfile;
	f->refs--;
	
	if (f->refs == 0) {
		file *tmp;
		bool in_list = false;
		rlist_foreach_entry(tmp, &file_list, in_file_list) {
			if (tmp == f) {
				in_list = true;
				break;
			}
		}
		if (!in_list) {
			free_file_blocks(f);
			delete f;
		}
	}
	
	file_descriptors[fd] = NULL;
	delete desc;
	
	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

int
ufs_delete(const char *filename)
{
	file *f = find_file_by_name(filename);
	if (!f) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	rlist_del_entry(f, in_file_list);
	
	if (f->refs == 0) {
		free_file_blocks(f);
		delete f;
	}
	
	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	filedesc *desc = find_filedesc_by_fd(fd);
	if (!desc) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
#if NEED_OPEN_FLAGS
	if (desc->flags == UFS_READ_ONLY) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	
	file *f = desc->atfile;
	size_t old_size = f->size;
	
	if (new_size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	
	if (new_size == old_size) {
		ufs_error_code = UFS_ERR_NO_ERR;
		return 0;
	}
	
	if (new_size < old_size) {
		size_t blocks_needed = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
		if (new_size == 0)
			blocks_needed = 0;
		
		block *b, *tmp;
		size_t block_count = 0;
		rlist_foreach_entry_safe(b, &f->blocks, in_block_list, tmp) {
			if (block_count >= blocks_needed) {
				rlist_del_entry(b, in_block_list);
				delete b;
			}
			block_count++;
		}
		
		f->size = new_size;
		
		filedesc *fd_iter;
		for (size_t i = 0; i < file_descriptors.size(); ++i) {
			fd_iter = file_descriptors[i];
			if (fd_iter && fd_iter->atfile == f && fd_iter->pos > new_size) {
				fd_iter->pos = new_size;
				if (new_size == 0 || rlist_empty(&f->blocks)) {
					fd_iter->current_block = NULL;
					fd_iter->offset = 0;
				} else {
					size_t block_idx = new_size / BLOCK_SIZE;
					if (new_size % BLOCK_SIZE == 0 && block_idx > 0)
						block_idx--;
					
					fd_iter->current_block = rlist_first_entry(&f->blocks, block, in_block_list);
					for (size_t j = 0; j < block_idx; ++j) {
						rlist *next = fd_iter->current_block->in_block_list.next;
						if (next == &f->blocks) {
							fd_iter->current_block = NULL;
							break;
						}
						fd_iter->current_block = rlist_entry(next, block, in_block_list);
					}
					fd_iter->offset = new_size % BLOCK_SIZE;
					if (fd_iter->offset == 0 && new_size > 0)
						fd_iter->offset = BLOCK_SIZE;
				}
			}
		}
	} else {
		size_t blocks_needed = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
		size_t current_blocks = (old_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
		if (old_size == 0)
			current_blocks = 0;
		
		for (size_t i = current_blocks; i < blocks_needed; ++i) {
			block *b = allocate_block();
			memset(b->memory, 0, BLOCK_SIZE);
			rlist_add_tail_entry(&f->blocks, b, in_block_list);
		}
		
		f->size = new_size;
	}
	
	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

#endif

void
ufs_destroy(void)
{
	file *f, *tmp;
	rlist_foreach_entry_safe(f, &file_list, in_file_list, tmp) {
		rlist_del_entry(f, in_file_list);
		free_file_blocks(f);
		delete f;
	}
	
	for (size_t i = 0; i < file_descriptors.size(); ++i) {
		if (file_descriptors[i]) {
			delete file_descriptors[i];
		}
	}
	
	std::vector<filedesc*> empty;
	file_descriptors.swap(empty);
}
