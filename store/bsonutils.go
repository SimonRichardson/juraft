package store

import (
	"strings"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
)

func isBSON(v interface{}) bool {
	if _, isBSOND := v.(bson.D); isBSOND {
		return true
	} else if _, isBSONM := v.(bson.M); isBSONM {
		return true
	}
	return false
}

// convertToBSONMap converts a bson.M or bson.D value to a bson.M
func convertToBSONMap(v interface{}) bson.M {
	// Already a bson.M
	if m, isBSONM := v.(bson.M); isBSONM {
		return m
	}

	d, isBSOND := v.(bson.D)
	if !isBSOND {
		return nil
	}

	m := make(bson.M)
	for _, docElem := range d {
		m[docElem.Name] = docElem.Value
	}
	return m
}

// applyMongoUpdate merges a bson.D or bson.M value containing one or more
// mongo field update operations into an existing bson.M value and returns back
// the result or an error if the operation cannot be performed.
//
// If dryRun is set to true, the function will only check that the update can
// be applied without actually mutating dst.
func applyMongoUpdate(dst bson.M, update interface{}, dryRun bool) (bson.M, error) {
	var err error

	if updateM, isBSONM := update.(bson.M); isBSONM {
		for operator, updateValue := range updateM {
			if dst, err = applyMongoUpdateOperator(dst, operator, updateValue, dryRun); err != nil {
				return nil, err
			}

		}
		return dst, nil
	}

	updateD, isBSOND := update.(bson.D)
	if !isBSOND {
		return nil, errors.Errorf("applyMongoUpdate: update argument should be bson.D or bson.M")
	}

	for _, updateElem := range updateD {
		if dst, err = applyMongoUpdateOperator(dst, updateElem.Name, updateElem.Value, dryRun); err != nil {
			return nil, err
		}
	}

	return dst, nil
}

func applyMongoUpdateOperator(dst bson.M, operator string, updateValue interface{}, dryRun bool) (bson.M, error) {
	if operator != "$set" {
		return nil, errors.Errorf("unknown/unsupported mongo field update operator %q", operator)
	}

	var err error

	// Check if the value is a bson.M or bson.D
	if setM, isBSONM := updateValue.(bson.M); isBSONM {
		for path, setValue := range setM {
			if dst, err = bsonSetPathRecursive(dst, path, setValue, dryRun); err != nil {
				return nil, err
			}
		}
		return dst, nil
	}

	setD, isBSOND := updateValue.(bson.D)
	if !isBSOND {
		return nil, errors.Errorf("applyMongoUpdateOperator: $set argument must be a bson.M or bson.D value")
	}

	for _, setElem := range setD {
		if dst, err = bsonSetPathRecursive(dst, setElem.Name, setElem.Value, dryRun); err != nil {
			return nil, err
		}
	}

	return dst, nil
}

func bsonSetPathRecursive(dst bson.M, path string, value interface{}, dryRun bool) (bson.M, error) {
	segmentIndex := strings.IndexRune(path, '.')
	if segmentIndex == -1 { // direct path
		if dryRun {
			return dst, nil
		}
		dst[path] = value
		return dst, nil
	}

	// Check whether the next segment in the path is present and whether
	// it is an object that we can recursively merge.
	curSegment := path[:segmentIndex]
	subPath := path[segmentIndex+1:]

	pathVal, exists := dst[curSegment]
	if !exists {
		pathVal = make(bson.M)
	}

	pathValM, isBSONM := pathVal.(bson.M)
	if !isBSONM {
		return nil, errors.Errorf("cannot create path %q in non-object field %q", subPath, curSegment)
	}

	pathValM, err := bsonSetPathRecursive(pathValM, subPath, value, dryRun)
	if err != nil {
		return nil, err
	}

	// Insert the nested map into dst at the current level
	if !dryRun {
		dst[curSegment] = pathValM
	}
	return dst, nil
}

func bsonGetPathRecursive(dst bson.M, path string) (interface{}, error) {
	subPath := path
	subDoc := dst
	for {
		segmentIndex := strings.IndexRune(subPath, '.')
		if segmentIndex == -1 { // direct path
			val, found := subDoc[subPath]
			if !found {
				return nil, errors.NotFoundf("field %q in path %q", subPath, path)
			}
			return val, nil
		}

		// Check whether the next segment in the path is present and
		// is an object
		curSegment := subPath[:segmentIndex]
		subPath = subPath[segmentIndex+1:]

		subPathVal, exists := subDoc[curSegment]
		if !exists {
			return nil, errors.NotFoundf("field %q in path %q", curSegment, path)
		}

		subPathValM, isBSONM := subPathVal.(bson.M)
		if !isBSONM {
			return nil, errors.Errorf("cannot visit path %q in non-object field %q in path %q", subPath, curSegment, path)
		}

		// Descend into the subDoc
		subDoc = subPathValM
	}
}
