CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

main: y.tab.o lex.yy.o main.o OpTreeNode.o Database.o Statistics.o Comparison.o DBFile.o File.o Record.o RelOp.o Schema.o Pipe.o BigQ.o ComparisonEngine.o Function.o
	$(CC) -o main y.tab.o lex.yy.o main.o OpTreeNode.o Database.o Statistics.o Comparison.o DBFile.o File.o Record.o RelOp.o Schema.o Pipe.o BigQ.o ComparisonEngine.o Function.o -lfl -lpthread
	
main.o: main.cc
	$(CC) -g -c main.cc

Database.o: Database.cc
	$(CC) -g -c Database.cc

OpTreeNode.o: OpTreeNode.cc
	$(CC) -g -c OpTreeNode.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc
    
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc
    
BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc
    
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
