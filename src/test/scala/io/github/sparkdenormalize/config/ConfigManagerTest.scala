package io.github.sparkdenormalize.config

import io.github.sparkdenormalize.UnitSpec

class ConfigManagerTest extends UnitSpec {
  // ----------------
  // SHARED VALUES
  // ----------------
  // TODO: put YAML strings here
  //  - OR: put them in the `src/test/resources` folder

  // ----------------
  // TESTS
  // ----------------
  behavior of "ConfigManager"

  it should "fail to parse invalid YAML" in {

    // TODO: make tests for malformed YAML

  }

  it should "correctly parse YAML for HDFSLikeDataSource" in {

    // TODO: make new test for EACH resource type
    //  - do them in the order in which the resources depend on each other
    //  - missing (required) sections should fail
    //  - incorrect types should fail
    //  - default values should be set

  }

  it should "expose HDFSLikeDataSource contained in it" in {

    // TODO: make new test for EACH resource type

  }

}
