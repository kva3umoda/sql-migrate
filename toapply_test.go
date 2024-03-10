package migrate

import (
	"sort"

	//revive:disable-next-line:dot-imports
	. "gopkg.in/check.v1"
)

var toapplyMigrations = []*Migration{
	{Id: "abc", Up: nil, Down: nil},
	{Id: "cde", Up: nil, Down: nil},
	{Id: "efg", Up: nil, Down: nil},
}

type ToApplyMigrateSuite struct{}

var _ = Suite(&ToApplyMigrateSuite{})

func (*ToApplyMigrateSuite) TestGetAll(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "", Up)
	c.Assert(toApply, HasLen, 3)
	c.Assert(toApply[0], Equals, toapplyMigrations[0])
	c.Assert(toApply[1], Equals, toapplyMigrations[1])
	c.Assert(toApply[2], Equals, toapplyMigrations[2])
}

func (*ToApplyMigrateSuite) TestGetAbc(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "abc", Up)
	c.Assert(toApply, HasLen, 2)
	c.Assert(toApply[0], Equals, toapplyMigrations[1])
	c.Assert(toApply[1], Equals, toapplyMigrations[2])
}

func (*ToApplyMigrateSuite) TestGetCde(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "cde", Up)
	c.Assert(toApply, HasLen, 1)
	c.Assert(toApply[0], Equals, toapplyMigrations[2])
}

func (*ToApplyMigrateSuite) TestGetDone(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "efg", Up)
	c.Assert(toApply, HasLen, 0)

	toApply = toApply(toapplyMigrations, "zzz", Up)
	c.Assert(toApply, HasLen, 0)
}

func (*ToApplyMigrateSuite) TestDownDone(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "", Down)
	c.Assert(toApply, HasLen, 0)
}

func (*ToApplyMigrateSuite) TestDownCde(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "cde", Down)
	c.Assert(toApply, HasLen, 2)
	c.Assert(toApply[0], Equals, toapplyMigrations[1])
	c.Assert(toApply[1], Equals, toapplyMigrations[0])
}

func (*ToApplyMigrateSuite) TestDownAbc(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "abc", Down)
	c.Assert(toApply, HasLen, 1)
	c.Assert(toApply[0], Equals, toapplyMigrations[0])
}

func (*ToApplyMigrateSuite) TestDownAll(c *C) {
	toApply := toApplyMigrations(toapplyMigrations, "efg", Down)
	c.Assert(toApply, HasLen, 3)
	c.Assert(toApply[0], Equals, toapplyMigrations[2])
	c.Assert(toApply[1], Equals, toapplyMigrations[1])
	c.Assert(toApply[2], Equals, toapplyMigrations[0])

	toApply = toApply(toapplyMigrations, "zzz", Down)
	c.Assert(toApply, HasLen, 3)
	c.Assert(toApply[0], Equals, toapplyMigrations[2])
	c.Assert(toApply[1], Equals, toapplyMigrations[1])
	c.Assert(toApply[2], Equals, toapplyMigrations[0])
}

func (*ToApplyMigrateSuite) TestAlphaNumericMigrations(c *C) {
	migrations := byId([]*Migration{
		{Id: "10_abc", Up: nil, Down: nil},
		{Id: "1_abc", Up: nil, Down: nil},
		{Id: "efg", Up: nil, Down: nil},
		{Id: "2_cde", Up: nil, Down: nil},
		{Id: "35_cde", Up: nil, Down: nil},
	})

	sort.Sort(migrations)

	toApplyUp := toApplyMigrations(migrations, "2_cde", Up)
	c.Assert(toApplyUp, HasLen, 3)
	c.Assert(toApplyUp[0].Id, Equals, "10_abc")
	c.Assert(toApplyUp[1].Id, Equals, "35_cde")
	c.Assert(toApplyUp[2].Id, Equals, "efg")

	toApplyDown := toApplyMigrations(migrations, "2_cde", Down)
	c.Assert(toApplyDown, HasLen, 2)
	c.Assert(toApplyDown[0].Id, Equals, "2_cde")
	c.Assert(toApplyDown[1].Id, Equals, "1_abc")
}
