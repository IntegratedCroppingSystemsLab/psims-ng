CXX      = mpicxx
CXXFLAGS = -std=c++11
LDFLAGS  =

SOURCES = $(wildcard *.cpp)
OBJECTS = $(SOURCES:.cpp=.o)

OUTPUT  = scheduler

.PHONY: clean

all: $(OUTPUT)

$(OUTPUT): $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OUTPUT) $(OBJECTS)
