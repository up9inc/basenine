// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

import "fmt"

func IndexToID(index int) string {
	return fmt.Sprintf("%024d", index)
}
