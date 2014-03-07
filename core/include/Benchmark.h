static const int FFTW_SIZE = 262144;

class Benchmark {

public:

    Benchmark() {};

    virtual double speed() = 0;

};