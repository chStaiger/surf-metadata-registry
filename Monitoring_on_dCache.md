# Monitoring on dCache
To monitor data on dCache one needs to create a channel which listens to a folder and logs all activities in that folder.

This information will be used to identify the files which changes and which need to be updated in CKAN.


## Authentication

### netrc
You can create tokens when authenticated through netrc. Make sure you have an entry like that in your `~/.netrc` file:

```
machine dcacheview.<your server>.nl
login <your user name>
password <your password>
```

### Macaroon
When authenticating with a macaroon, it is important (for now) to **NOT** use the flag `--chroot`:

```
get-macaroon --url https://<webdavserver>:2880/pnfs/<path to folder> \
   			 --duration P7D \
			 --user <username> \
			 --permissions DOWNLOAD,UPLOAD,DELETE,MANAGE,LIST,READ_METADATA,UPDATE_METADATA,STAGE \
			 --output rclone  <token file name>
```

Diasadvantage is: you will have to address all your data by their absolute path.

## Create a channel

You can create a channel that will listen to events like this:

```
ada --tokenfile <token file name>.conf --events tokenchannel /pnfs/<path to folder>
```

To list all available channels:

```
ada --tokenfile  <token file name>.conf --channels
```

## Prepare your data

We do not want to act on each event on any file, but only those which are registered in CKAN. To find them you need to create a label and attach it to the files in question. Here we use the label `test_ckan` and attach it to one file in the folder that is monitored by the channel. 

We will label:

```
ada --tokenfile  <token file name>.conf  --list .

TwentyThousandLeaguesUnderTheSea.txt```

ada --tokenfile  test-macaroon-token.conf \
    --setlabel TwentyThousandLeaguesUnderTheSea.txt test_ckan

```

If you already created a channel for the directory you will see this logging information:

```
inotify  /pnfs/<path to>/TwentyThousandLeaguesUnderTheSea.txt  IN_ATTRIB
```

We also need some state information about the data. Here we will use the checksum. Retrieve the checksum from dCACHE for the file and add it to your metadata in CKAN:

```
ada --tokenfile  test-macaroon-token.conf --checksum TwentyThousandLeaguesUnderTheSea.txt

TwentyThousandLeaguesUnderTheSea.txt  ADLER32=19ed54dc
```

In this tutorial I will add that information manually to CKAN:
1. Navigate to your entry in CKAN and hit the `Manage` button
2. Add a new `Custom Field` with key `checksum` and entry `[adler32, <value>]`
3. Click `Update Dataset`

## Actions to listen to

Now we labeled our data, when do we need to update our ckan?

1. The file is overwritten and the checksum changed

	⚠️Cannot be implemented⚠️
	A file in dCache is only updated through a reupload when the content differs. However, in a reupload the file is deleted with its labels and recreated without labels. Hence, the cue to listen to is gone.

2. The file is moved
	The cues are `IN_MOVED_FROM ` and the old name, and `IN_MOVED_TO` and the new path. 
	
	Next we need to check the label which we will find in the `stat` under the section `labels`:
	```
	ada --tokenfile  test-macaroon-token.conf --stat <new name>.txt
	```
	If our label appears in there, we need to update the location in CKAN.
	
	**Channel output**
	
	```
	inotify  /pnfs/<path to>/TwentyThousandLeaguesUnderTheSea.txt  IN_MOVED_FROM  cookie:xuOcESo9Aoz80JuGW8gKxg
inotify  /pnfs/<path to>/Twenty_old.txt  IN_MOVED_TO  cookie:xuOcESo9Aoz80JuGW8gKxg
	```


3. The file removed

	Listen to the `IN_DELETE` cue, check if this file was registered in CKAN. Since the file is gone, we cannot check the label, but we have to check if the location existed in CKAN and  update the CKAN entry with a remark that the file no longer exists.
   
   **Channel output**
   ```
   inotify  /pnfs/<path to>/Alice1.txt  IN_DELETE
   ```
   
   
## Example workflow

In this workflow we show how you can monitor a folder with data which has been referenced in CKAN.

We use a folder `/pnfs/<path to>/cs-testdata`.

Before we can use `surfmeta` to interact with dCache, we need to tell the client with which method we want to authenticate with dCache and where to find the authententication file:

```
surfmeta dcache auth --netrc ~/.netrc

# or

surfmeta dcache auth --macaroon mytoken.conf 
```
This informstion will be saved and if your authentication does not change, e.g. you token expires or you need to access a different location, you will not have to re-authenticate.

Now we can sart the workflow:

1. Start the listener:

   ```
   surfmeta dcache listen --channel test-api /pnfs/<path to>/cs-testdata
   
   🎧 Listening to dCache events on '/pnfs/<path to>/cs-testdata' (channel: test-api) …
   ```

2. Upload a file to that location and label it with `ckan-test`. This label is the default label for the `surfmeta` command. If you use`ada`, please check the spelling!


   ```
   rclone --config=test-macaroon-token.conf copy TheHuntingOfTheSnark.txt test-macaroon-token:
   ```
   
   Confirm that the data is there:
   
   ```
   ada --tokenfile  mytoken.conf --list /pnfs/<path to>/cs-testdata
   ```
   
   Label the data:
   
   ```sh
   surfmeta dcache addlabel /pnfs/<path to>/cs-testdata/TheHuntingOfTheSnark.txt

    Running ADA command: ada --netrc /Users/christine/.netrc --setlabel /pnfs/<path to>/cs-testdata/TheHuntingOfTheSnark.txt test-ckan

   ✅ Label 'test-ckan' set successfully on '/pnfs/<path to>/cs-testdata/TheHuntingOfTheSnark.txt'
   ```
3.	Now you can register the data in CKAN:

	```
	surfmeta create /pnfs/<path to>/cs-testdata/TheHuntingOfTheSnark.txt --remote
    ⚠️ WARNING: --remote chosen: skipping checksum and system metadata.
    Creating metadata for /pnfs/<path to>/cs-testdata/TheHuntingOfTheSnark.txt.
    Enter the system name: dcache
    Dataset name: The Hunting of the Snark
    Author name: Christine

    📂 Available Organisations:
    1) book-club
    2) maithili-priv
    Select an organisation by number: 1
    Do you want to add the dataset to a group? [y/N]:
    🆔 UUID: 78ffbab3-1fe7-46ef-a440-9887d344626f 
    🌐 Name: The Hunting of the Snark
    ✅ Dataset created successfully!
	```
	Go to your CKAN instance and check the entry!

4. Let us try out what happens if we use the `ada` commands to **rename** the file:

	```
	ada --tokenfile  mytoken.conf \
    > --mv /pnfs/<path to>/cs-testdata.TheHuntingOfTheSnark.txt \
    > /pnfs/<path to>/cs-testdata/
	```
	
	In the listener you will see that the event is picked up and the listener informs us that some metadata has been updated:
	
	```
	/pnfs/<path to>/cs-testdata/TheHuntingOfTheSnark.txt
	/pnfs/<path to>/cs-testdata/book.txt
	Running ADA command: ada --netrc /Users/christine/.netrc --stat /pnfs/<path to>/cs-testdata/book.txt	
	✅ Successfully updated location for dataset '78ffbab3-1fe7-46ef-a440-9887d344626f'.
	```
	Check it on the CKAN server. Indeed the `location` entry changed.
	
5. Let us **delete** the file:
	
	```
	ada --tokenfile  mytoken.conf \
	--delete /pnfs/<path to>/cs-testdata/book.txt
	```
	
	Again the listener picks the event up:
	
	```
	/pnfs/<path to>/cs-testdata/book.txt
	🔴 Detected delete: /pnfs/<path to>/cs-testdata/book.txt
	✅ CKAN dataset '6edd3fe9-adc7-4da9-9269-af505f3e9aa5' updated with deletion warning.
	```
	
	Your CKAN entry now carries a warning tag, that the `location` does not exist any longer.

	
	
   
   
 

   



 
