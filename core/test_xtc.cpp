#include <iostream>
#include <fstream>
#include <stdlib.h>
#include "XTCWriter.h"

using namespace std;

int main() {

	ofstream output("test.txt", std::ofstream::binary);

	XTCWriter writer(output);

	vector<vector<float> > box(3, vector<float>(3));
	box[0][0] = 1.0; box[0][1] = 0.0; box[0][2] = 0.0;
	box[1][0] = 0.0; box[1][1] = 1.0; box[1][2] = 0.0;
	box[2][0] = 0.0; box[2][1] = 0.0; box[2][2] = 1.0;

	int natoms = 1234;

	for(int i=0; i < 100; i++) {
		vector<vector<float> > positions;
		for(int j=0; j < natoms; j++) {
			vector<float> coord(3);
			for(int k=0; k < 3; k++) {
				coord[k] = (float) rand()/ (float) RAND_MAX;
			}
			positions.push_back(coord);
		}
		writer.append(i, (float)i/10.0, box, positions);	
	}

}
