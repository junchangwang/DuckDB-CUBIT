


CREATE TABLE Person_workAt_Organisation(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, PersonId BIGINT NOT NULL, OrganisationId BIGINT NOT NULL, workFrom INTEGER, classYear INTEGER);
CREATE TABLE Message_replyOf_Message(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, messageId BIGINT NOT NULL, parentMessageId BIGINT NOT NULL);
CREATE TABLE Message_hasAuthor_Person(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, messageId BIGINT NOT NULL, personId BIGINT NOT NULL);
CREATE TABLE Message_hasTag_Tag(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, id BIGINT NOT NULL, TagId BIGINT NOT NULL);
CREATE TABLE Person_likes_Message(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, PersonId BIGINT NOT NULL, id BIGINT NOT NULL);
CREATE TABLE Message(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, id BIGINT, "language" VARCHAR, "content" VARCHAR, imageFile VARCHAR, locationIP VARCHAR NOT NULL, browserUsed VARCHAR NOT NULL, length INTEGER NOT NULL, CreatorPersonId BIGINT NOT NULL, ContainerForumId BIGINT, LocationCountryId BIGINT NOT NULL, ParentMessageId BIGINT, typeMask BIGINT);
CREATE TABLE Person_knows_Person(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, Person1Id BIGINT NOT NULL, Person2Id BIGINT NOT NULL);
CREATE TABLE Person_workAt_Company(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, PersonId BIGINT NOT NULL, CompanyId BIGINT NOT NULL, workFrom INTEGER NOT NULL);
CREATE TABLE Person_studyAt_University(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, PersonId BIGINT NOT NULL, UniversityId BIGINT NOT NULL, classYear INTEGER NOT NULL);
CREATE TABLE Person_hasInterest_Tag(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, PersonId BIGINT NOT NULL, TagId BIGINT NOT NULL);
CREATE TABLE Forum_hasTag_Tag(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, ForumId BIGINT NOT NULL, TagId BIGINT NOT NULL);
CREATE TABLE Forum_hasMember_Person(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, ForumId BIGINT NOT NULL, PersonId BIGINT NOT NULL);
CREATE TABLE Person(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, id BIGINT, firstName VARCHAR NOT NULL, lastName VARCHAR NOT NULL, gender VARCHAR NOT NULL, birthday DATE NOT NULL, locationIP VARCHAR NOT NULL, browserUsed VARCHAR NOT NULL, LocationCityId BIGINT NOT NULL, speaks VARCHAR NOT NULL, email VARCHAR NOT NULL);
CREATE TABLE Forum(creationDate TIMESTAMP WITH TIME ZONE NOT NULL, id BIGINT, title VARCHAR NOT NULL, ModeratorPersonId BIGINT);
CREATE TABLE University(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, LocationPlaceId BIGINT NOT NULL);
CREATE TABLE Company(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, LocationPlaceId BIGINT NOT NULL);
CREATE TABLE City(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, PartOfCountryId BIGINT);
CREATE TABLE Country(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, PartOfContinentId BIGINT);
CREATE TABLE TagClass(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, SubclassOfTagClassId BIGINT);
CREATE TABLE Tag(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, TypeTagClassId BIGINT NOT NULL);
CREATE TABLE Place(id BIGINT PRIMARY KEY, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, "type" VARCHAR NOT NULL, PartOfPlaceId BIGINT);
CREATE TABLE Organisation(id BIGINT PRIMARY KEY, "type" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, url VARCHAR NOT NULL, LocationPlaceId BIGINT NOT NULL, typeMask BIGINT);




