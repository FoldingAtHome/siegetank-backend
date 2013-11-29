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

	// For sanity, you must ensure that output is opened with std::ofstream::binary!
	XTCWriter(std::ostream &output, float precision=1000);

	void append(int step, float time, 
		        const std::vector<std::vector<float> > &box,
				const std::vector<std::vector<float> > &positions);

private:

	std::ostream &output_;
	const float precision_;

};