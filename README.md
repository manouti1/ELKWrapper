# ELKWrapper  

ELKWrapper is a .NET library for simplified interactions with Elasticsearch.

  

## Installation

Install the ELKWrapper library using NuGet Package Manager:

      
    dotnet add  package  ELKWrapper

## **Usage**

**Initialize ELKWrapper:**

    var elkWrapper  =  new  ELKWrapper<MyDocument>(settings, "myIndex", "myAlias");

**Indexing**

    var document  =  new  MyDocument  {  /*  Document  properties  */  };
    
    var documentId  =  elkWrapper.IndexDocument(document);

**Searching**

    var searchResults  =  elkWrapper.ScrollSearchAsync(
    
    q =>  q.MatchAll(),
    
    s =>  s.Descending(f =>  f.Timestamp),
    
    pageSize: 10,
    
    pageIndex: 0,
    
    scrollTime: "1m"
    
    ).Result;

**Alias Management**

    elkWrapper.CreateAlias("myAlias", "myIndex");

**Document CRUD  Operations**

    var retrievedDocument  =  elkWrapper.GetDocumentById("documentId");
    
    elkWrapper.UpdateDocument("documentId", updatedDocument);
    
    elkWrapper.DeleteDocument("documentId");

**Configuration**

Configure ELKWrapper  with  an  instance  of  ElasticSearchSettings:


    var settings  =  new  ElasticSearchSettings
    
    {
    
    Url =  "https://your-elasticsearch-cluster",
    
    Username =  "your-username",
    
    Password =  "your-password",
    
    // Add  other  settings...
    
    };

    var elkWrapper  =  new  ELKWrapper<MyDocument>(settings, "myIndex", "myAlias");

**Contributing**

Contributions are  welcome!  If  you  encounter  issues  or  have  suggestions,  please  open  an  issue  or  submit  a  pull  request.



