from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom


class Group(GraphObject):
    __primarykey__ = "number"

    number = Property()
    mastqq = Property()
    date = Property()
    title = Property()
    groupclass = Property()
    intro = Property()

    members = RelatedTo("Person", "Member")


class Person(GraphObject):
    __primarykey__ = "qq"

    qq = Property()
    nick = Property()
    age = Property()
    gender = Property()
    auth = Property()

    groups = RelatedTo("Group", "In_Group")
