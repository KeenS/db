#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "microdb.h"

Result
initializeFileModule()
{
  return OK;
}

Result
finalizeFileModule()
{
  return OK;
}

File
*openFile(char *filename)
{
  File *file;
  file = malloc(sizeof(File));
  if( file == NULL){
    ERROR("Memory full");
    file->fd = -1;
  }
  for(size_t i = 0; i < strlen(filename)+1; i++){
    file->name[i] = filename[i];
  }
  
  file->fd = open(file->name, O_RDWR);
  if(file->fd == -1){
    ERROR("Failed to open file");
  }    

  return file;
}

Result
createFile(char *filename)
{
  if(creat(filename, 0644) == -1){
    ERROR("Failed to create file");
    return NG;
  }else{
    return OK;
  }
  
  
}

Result
deleteFile(char *filename){
  if(unlink(filename) == -1){
    return NG;
  }else{
    return OK;
  }
}

Result
readPage(File *file, int pageNum, char *page)
{
  lseek(file->fd, pageNum * PAGE_SIZE, SEEK_SET);
  if(read(file->fd, page, PAGE_SIZE) < PAGE_SIZE){
    ERROR("Failed to read 'page'.");
    return NG;
  }else{
    return OK;
  }
}

Result
writePage(File *file, int pageNum, char *page){
  lseek(file->fd, pageNum * PAGE_SIZE, SEEK_SET);
  if(write(file->fd, page, PAGE_SIZE) == -1){
    ERROR("Failed to write 'test'.");
    return NG;
  }else{
    return OK;
  }

}

Result
closeFile(File *file)
{
  if( close(file->fd) == -1){
    ERROR("Cannot close file");
    return NG;
  }else{
    free(file);
    return OK;
  }
}

int
getNumPages(char *filename){
  File *f;
  f = openFile(filename);
  if(f->fd == -1){
    return -1;
  }
  int result;
  result = (lseek(f->fd, 0,  SEEK_END) + PAGE_SIZE - 1) / PAGE_SIZE;
  return result;
}
