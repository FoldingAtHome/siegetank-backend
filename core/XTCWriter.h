#include <iostream>
#include <vector>

/* -*- mode: c; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- 
 *
 * $Id$
 *
 * Copyright (c) Erik Lindahl, David van der Spoel 2003,2004.
 * Coordinate compression (c) by Frans van Hoesel. 
 * C++ Writer (c) by Yutong Zhao <proteneer@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 */

class XTCWriter {

public:

	XTCWriter(std::ostream &output, float precision);

	void append(int step, float time, 
		        std::vector<std::vector<float> > &box,
				std::vector<std::vector<float> > &positions);

private:

	std::ostream &output_;
	const float precision_;

};