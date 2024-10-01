import boto3
import requests
from progress.bar import Bar

class ProgressFileObject:
    def __init__(self, fileobj, callback):
        self._fileobj = fileobj
        self._callback = callback
        self._read_bytes = 0

    def read(self, amt=None):
        chunk = self._fileobj.read(amt)
        if chunk:
            self._read_bytes += len(chunk)
            self._callback(self._read_bytes)
        return chunk

    def __getattr__(self, name):
        return getattr(self._fileobj, name)

def upload_to_s3(url, bucket):
    # Get the file name from the URL
    file_name = url.split('/')[-1].split('?')[0]
    object_name = f"CoVoST/{file_name}"
    
    # Download the file
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        # Total size of the file
        total_size = int(response.headers.get('content-length', 0))
        
        # Create a progress bar
        bar = Bar('Uploading', max=total_size)

        # Progress callback function
        def progress_callback(bytes_transferred):
            bar.goto(bytes_transferred)

        # Wrap the response.raw file-like object
        wrapped_file = ProgressFileObject(response.raw, callback=progress_callback)
        
        # Upload to S3
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(wrapped_file, bucket, object_name)
        
        # Finish progress bar
        bar.finish()
        print(f"Uploaded {file_name} to S3 bucket {bucket} as {object_name}")
    else:
        print(f"Failed to download {url}")
# List of URLs

# These urls keep changing on a regular or some interval basis so execute the data-downloadcovost.js file to create a output.json file and then copy paste that here for the urls from output.json
urls = [
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-18.0-2024-06-14/cv-corpus-18.0-2024-06-14-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055024Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=90f4435a327c99e409b74a8cb51e8264c63aaf0f46d721a2cadb7bb8e0886c7e7741eb8352fa475d8e39faaa1ae3fd0e320a26ab7ccb24401dcace49ecbbb633517a215a25fe62ce4506a0ce580891770a25ec4cabcac07fff33a2d888ccfa6814e56f9d38f10c07e9c72b9723156c04356399e3f6a01d2fec0186e36c5475eda583b2802faf1bc888d79817e1c4d826cb8afd13f957995515cdfc923a88301b455d3682ee57d74bbf0fca5d22ca3f814017d46378c6b8bd68fd2c11fd24c6ac6891ea54bc474ffe275dade0759cc1e19515a9918f8e5f27a55558ee475835d9bfaa7c221737e9b5ed6b60e2f9d370441e549448195ff9f2085e4e26b4cfaba8",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-18.0-delta-2024-06-14/cv-corpus-18.0-delta-2024-06-14-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055039Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=a7aa939dc1e3846c33134f5e08cf80c6d113fa0ae09fcf895fcbfbdf3bae6728b24c07ef9c4d1e3e4ae47064f1d8018f8ccba77a9c395d32d7119c50a294e83889c1eb79f343cbf9e955234c7918363fa96a3a27347f54e1902156b4053f8c05a647015e220679745cafbdf63f40300e81615ed1049acc2be8448efef46656ee389756e2780f5b122ff42d589d3727a60b50a057606d1bc2be47360a6020391ef612b10c1ecd708b8096334bf69d8ddb7f2938d1b85ec31378cec62f6034754ff88691ef751f9acd0f6e35c6b8e2b237154ab498a0e5f3a40cb7fc7c0e673f3078fd8ca355415f81863bfa961fded3631bf2625834e2375e78c92729234f0113",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-17.0-2024-03-15/cv-corpus-17.0-2024-03-15-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055051Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=0b66ffcb0732caffbde64ced7b682a33b787e19a5238a3f5c99d578f5db99b8cdfe5b25ec3afad6758329b1cc88220c95240c8061c57d761ec92a1ef6d97263fd12b24d64d1052ed67a08997ad2e13f2d139e770c1d235f78eb2af01a1ee142e851d692ab3de7038856089b1256a73802bec0ed3c841c8602b9a1a421ca8f3d0e28200beacf78b4352981d5695b837fcf1428b0524eb54aa45dcfb0c46cb08221bdadffe512aa3a9ce01ecd3255ae3ea21f8a4d514883343157994f91e6674186cb7fcfacf4fe238f8f969f09eb8cc23f5ab6d0ed7ed9f09d7b5e1fbd75085c62a3ed079182e04cb9171e721ce8fee8505fcebb0488915a965279b002c16ea4a",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-17.0-delta-2024-03-15/cv-corpus-17.0-delta-2024-03-15-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055103Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=bf1aa190980c8057b73ea4d53f5c1135e3b07b4e29e8609b7a1d4d2565daa84f2d808d1e4a8e2dc258642443286b09e2dbf8fe87703f425076862c9319b82bc9d1e8e2381122e2f8b1a8d46ae5faed2f4f2e1343d8ba3a800eda74d7bac07e31f81ee93a2a8c2220a877469f2b2134ed1fb402bc2efe24287f70343bf631bf5ccdbe0f239f9e0b7010f463f7822640877b8084d6b57c34c8c66d00b2b3aec6a42af3133be7759185d89caac9bbd5f314ca20878a07cc13fb8ac18e889eb6ea5a05a931903756890be8aaaccfad3cb3dd09d318b2caf9ef6d39c30870f8774b58302212703473e523dbba2f07921f2f52d6fe49295f6b159653363d64cdc2fd4e",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-16.1-2023-12-06/cv-corpus-16.1-2023-12-06-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055115Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=9bdcdcec08e125679290670e50d49fb208537781ebd0765294abf9eb4e40fc2b2ecb58b87f04ae738fd674bdb219a86e78944d75836b2390e383d1d8e61b40a35620f73ba2829d2bc750acdc625c68431de1c58eb780eaff04e1a98912253f5d60f01dba7d32893e2f3917302bae310f18d49a735904a6a84bbf9f82bce6ef676cbd1b086349b72aabb71bef70a49e5c947e14d93280f724dd20fb380cabc9a728a15e61279bf8ae8a6a9f1a4db741a7828bbb49a30e9fd4bc71fad75b9a3880d660cc7220b7479a75e009265d38c622dba3c2e48ab52a74bf7aa86993407e842ea44e723be502d2fd4aa11dc35385eb2c3b272b3e7ae02c16b1767df1cbcdca",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-16.1-delta-2023-12-06/cv-corpus-16.1-delta-2023-12-06-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055128Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=92a244e04b7da1c59cadfb8fffad992da8c35215d1f98f88ced179400e5d9be5cf2c91855ae8cb2cda74b29985a0e276f691527d3688ce116a2f3f177c827b0813672969fdeff46e5d2d7400d59d448a2825f84c6b499700ea7affc5460bdb7e4d12d60cb953b61d204a48eb0d39c047d760868b6af52fdc919c530908c5f1319ac30250e22108521da9e789b5d98f8cddc622617a207a1ef6e4ee9ecf48dd831ac4554061deff5b551e5ca09dd863f3220c73ea08bbf8632fffdebf748e0009b7de877e8f7571a3b39683a59aa852b236df1cff2b9128bf2b63f2b3910284fcabba44ee3601c63d47b7ccb01ff1dcd8acba443329bc28ffe59ce0ad928b204a",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-15.0-2023-09-08/cv-corpus-15.0-2023-09-08-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055140Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=8796b2b776e96f4647afd7f5622affc16ba8675937030964e704d896ef91b83cd312b9d7c3da2b0ff2eca1acaf50f08befc37f0c5f0ca2af4d1ae9fdbe7c179e8f7e3acd53c8675f56f1ac7cca7ea8e542143116ae3dd11ea8ae0274a56acc02a5abe6abd57b818d3027e921fe1d11cdb28d74845a1d020f704678cd22f18e4782d82670f97d3ee95bf74829655c4bfbd2901dab84d6d0680edabc57e447b09ebb988095b087a86f59c6adfaec37c115619b321c34b689e75118492946fd2f9dadb49cc3c7ca7acfb3ea4194d5c46fafdf4ce7b69a91f103c4280efb64853a2d8fa76eaba426d8ffacf22adbaeacb61d4ad3a6a490086fffb38e612c279d1494",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-15.0-delta-2023-09-08/cv-corpus-15.0-delta-2023-09-08-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055152Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=8e0103ce384331ada8074ddb5c6c1906979a39503c097a515f993836ed22b33b9b4c30bfd458c0d15ecfcda43bc1f0eb29c99038f93d1ea56fb07cc8cd04ad03802845ad4553bcca835fd1af1fcd73ad7ed6f1af36e40b4168164d714971f733424ac1934ba1d64a22add36f18abf868b9593bb2e4eb1e7a71d10e19e16f5269c64ed01ca9c4bf685a640cad16cfb841487e028dce45b1298c6ca4d70726f731f70ff5c5f1a6a1b2d00ea27c88f32e0f89dc9dcc7d32860356410096c536f3996982605c7003d3a49d77ff2b5a98d8011eb244c6c2b1a60a465630faae11808f1befcfc6010127bd685e0af6a87a194f693e59d32f0a975435c561df146e59fd",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-14.0-2023-06-23/cv-corpus-14.0-2023-06-23-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055204Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=3dc70af373c320a266bbe8ab6ce21c51584b88e299994a706814598d4e7495fc639c869abff51c421a3cc35242e4b6e9e3889dd367416b06cfcfcda56fe9d6b8a1d5f3e98e42dfafc8fe413dc8b8d81cef2c2d42498409e39ab02e54238f89ac7130b011ba68509132c601e7bbf77180be5392a428e323dafbd70e34b1f6a4769adb9465acc4872bca73bfd681ca8e7ce000b6f8c823469cf891a5a621741817aedc7616ae9ac5bbeec9e154926fa06d34e3531d56cbb824f314b26f211e410e285bccaf0af89643167eca7384a7043a2a0b9fdc973cbcb6d44a642228e7008a86fa6c2095a9990ff6639bbb5453fc73b97ca2b421324b4fc6abfd90d57a433c",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-14.0-delta-2023-06-23/cv-corpus-14.0-delta-2023-06-23-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055216Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=87b973948583092e77f4d35a438f660118f651e77c713dcb7d400ddc0186fc1da3abc51c28e9e2dbbac980b310b500795ff4a2366b52707ce4527fa4c2e376a14d8e1750b694ecc76b3ab24e48dce1c70cf794084c712e466a940cfdbc20a5b2848d9144afbbbe2da0573d95c07b3d2714e0419f369cac5d5bbe7b3bbf497e89110a56bddc695e2029f868cc6d66795268087d0c10e53d09779570bab73dcdafde2ac5cbffb87b17d4dc61c14300776d86d678e8b1720cbd99f4992208e2cae4df02b2fde6a62458dc90492bcef7c7ff006f2b9834d5df500ce23e7a645e3227a1e84391b19dc6eea5e98a7ef878fa1167d455dd723e427de91f0427698872d3",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-13.0-delta-2023-03-09/cv-corpus-13.0-delta-2023-03-09-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055228Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=2471da2221802575b89bbe3eb431fdf1b3460c5659edafb70d0eac1b7072296d0a08b7442afaf55ec49c85bb0fae006cc78118ff961e45d44b985c7d03dcfd4e74f3f6b68ac65ebea8fe6e04ffc94403ba1edd65a2b6a3ae378f26f5c122fdd32b43cec7a38a3ffa5c96ed670c963ab3fa99739030d1ff54741ec9a328b3c23f7ec94c82bd93b4bb1816fe0c2800dee90c2e6b0129b3d96a4fdc6e4ed5209e01a58d7a8775145cd5244c821b122c13eb6f98afc2d4539b9f3a97ff86d66575733563fc243698fea79eab8a22e0d1a23d45d3cbc14ed7b53554d7f2aa9706550fc74edfb23e8230fff33703deaaefa9560f3f7c85844983174d02025f30e63ea3",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-13.0-2023-03-09/cv-corpus-13.0-2023-03-09-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055240Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=6f9c01687ab6b18512731eb6b884c7ca86c7a459677516abbafb6285b4e2b291c29d8f58bc13b0fac103df72c09134ebca6e6104e73e01bd4ab6ad5bcadcc9478e69c32d734a44f1be3bc595557315a53b41c20ea533a6f3b305f2898a66f829698a62a1e0b133ca7e69fa30be9429434b2f3b91e27bb5e229f8d151b8a1f0cdeb422c363451fb5bd3c40f5b49900e89ba01e288f962467ea6ee4ad1b064ce43c6bef77c52c09552b63937721ff89eaf415e081be8bf485421da1f485b931ac692a95dd525b45fdd6c6b7f6d4226134ced81ab4f71d97e281beeebaf05f13d499a40eeaf685bad01413db3ec3915d890fd2f4731861b9c84169a673a13e0c4f7",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-12.0-delta-2022-12-07/cv-corpus-12.0-delta-2022-12-07-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055252Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=4a018522514f4ea041ce51bd0cd046f129b9ade89746c35347949f609281be29ab0cb80c7e3d37f4c3b121584888680c6be0e89e1e90ce63ca063da6186a8f9187e877e70756835024caf72461fb91b9cfdeb4cfd53eda5192afa21f08f14fe0cc01d75e16ee766bfdbd7165573f55fc49032f048561b4b58e9cabe513a0f5b335539e56fb575731f43bef5c8191b3107bd0d128d10c17b12ccb62361fa1a24ba3281b10b3a895806449eecba0856b3429925f454386ac75a63664e5ddb1bd5290fb208154fd3928612589b4610e4618066b92d74ce6d3096daec9958175100e41c2c001c50f824781bc3663107be83987368d128e9a2b7e0e1701d0b77a1791",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-12.0-2022-12-07/cv-corpus-12.0-2022-12-07-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055304Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=60cfa47e6a9f92e44ec5701d7c1ff710103918a52446a2e4ed080a462902f0814ce19f38297c7ad6328e4ac7a20cdfcc4c6e2785a8de4e99d5a7f18139c62112b71a0c6b256558b80674d1a1592cfa1963a105ae1e6f1c7a89aef13f95791e758851845b0ab7b1970d829a23d62f1c583a2ebcabfd262c04483d80e6f0215340259071ca5d092372b57497944e1c13f6b3c5b82cc4ea0fb6dc55278d344ec16f1ad83476fcf394e6da46c0ae46b60fcb08caa9b7913c2d668edb8e8b5d1c8bdc03f0249652a2210307a43a7329101e9f4adb11fbc217ef7d4fb4be4f03bc653554bf448101d994b3700ffcdd1cd502df1a64cb6ffb2d3819c70712218aab1b2b",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-11.0-delta-2022-09-21/cv-corpus-11.0-delta-2022-09-21-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055316Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=62ede811259b1c117ff0f8481cff1892caec851e4e36da727d92b48532247343cd6e0f5b6a1849f9ebc7236132f6094ff6867fbb0fb1a950a51655023e0ef70c89280fde1b1094b48a6f50641d95ffb80b869765cc89980e2d46446e98bfdf495289eab22a02c1feb72390b10b190e99b2d3ad888728ad77310068704161b2d71d1dbe23fd1824d03dcd635f3747031f81c01e7a7f3d24547e68e59a6a0d80cb0df504e9cf9d11a3b05c73a445aec6b7421d2345a36ba12c14ad42387c0f817dbac742114b4f4458bd269c906bd862b9c1943b793a7bb4e46c68d28c8f32c23c288362304afb9daf556f8041610a988c051549334c3910f5fe20778395e915cd",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-11.0-2022-09-21/cv-corpus-11.0-2022-09-21-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055329Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=18b65588861948c5db54fa034a027b5903da5a63da38ff450fda69f4ee458873831cc425537b3a765df77771cb413a0b42fa41c5461cc9f62ee82d2d2764475455b2b8044b81a983fb463132ccac926c101be5bbc779eecc31b108982f5011e64074afd1ae880ccfc07b3dd7b5beffed2ba51db5e5d0f735876df7e3b0c66aca7e544c0f0723e7aeea6b99248e120195d5ae5c2481ca6cae2d0ea36c9ef6fa04ca413572637523cc7684625c838eac5df236a39bebd2511cdda39f1c2149c5a658b63fb54577026dd1b94478023a8b133ca501cbb732e9b9767816c7ce18f8694cf0e0a3788ba343760fa3aafb34a975eb2359dc6840eae08d2531bf46a0d782",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-10.0-delta-2022-07-04/cv-corpus-10.0-delta-2022-07-04-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055341Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=4f6438ac34f1ab40443157ab060c29a79ffe2452ed9f417f92f8a9f4b3a1008f12486c0bcae2f349dc6d076f36c1f27f07f522e2d299d65bab16986bad4a2561b4ca64ea0f40250fcbb102f4d144d6a40237c679c2ae9b93d42356d2bef28cf431c4a1000dbc94e795011c25b70266134cc65b5d0e48183651873577e912927683229ca5c4d0b1511857956a460120d49de4ac5adf55ef6103cfb125b7021c1049e07de0ece5b382a7ce902d34e9ddfaa75ef555edf1051cdaac2d525b03ecf85ce10bddf642936e8cb4d11c2083d2e2a9cf6b9f231e2fdf8022226449653f7d4003e4d0a72c8016470765d887e169ce831e6d390da374f0ed3106ce17d69b64",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-10.0-2022-07-04/cv-corpus-10.0-2022-07-04-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055353Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=5d6c003fa3a6734d23a62335676ace8fcf53b7c143704c70ea9e343175af03bfb80541f9fa533e478e1f2d6fd16c1b503075bc0b38bfec94590f5a17c417756fb42e66db6c06b3b778b26d3d3793816d0e0551aa512953ed76cd7e47cc8ee3e4f7f3474c523fe4348af7a4b3dc3dc615bd6299f791635795d84792483e3537a6785258092bca35f16f0ede6461ccfc366ed3af298e9964393efb7aa0407daf74d5a84e24a78b8366218d216c8e191505c8b43af47e62a90168ff185b9971aa773ec3053fe1663b138d22e5d827df7dd02c26660020fa352eb5eec8f7d435f002b3fa0039806e9c69b45c67c147983d907c76be269ff8453b249b28152ef6199c",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-9.0-2022-04-27/cv-corpus-9.0-2022-04-27-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055405Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=02be90592fa4e01d00cf395f3ddfe0e5c891b0ee4c87c08a8f7692634a035501f7a5917c0d74f4875c21471d8e0d5fcbc09c6c45e2ca861b368a58e1dc39743d0769067a268f0fe9262f08146ffd4e130c9d54e5f0cd6cba892602dd6c47fbe9a8373994f1cb597ca3ae76f27160334071ab35c78a9b5c1b20eeb3780aee8ba89b0de20fb5be281d75c2962d380dfe155ae11f89a96db60cc87022441776da32f1b7b1dfc90b6f4ca4adc3f12201b4cfc4c8c93c243d759b707b210d0570f947759b754e714854d564d3403a254596f52cb9dc30c9946c44429dbe625b9eb15c8db888f141b05dc7e2c30ee19e09aedcce4c373d85cbbc0243ac3301251e932a",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-8.0-2022-01-19/cv-corpus-8.0-2022-01-19-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055417Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=30a4b9be269a9af3fb87119705d57c7c03fafcd0b41c2f69531b4681dcb5cdd6f983379132f42178acf8a536264bbf8df89e03e03f3695441577b67dbf8d9cd8c8e0ca4ea78017b5a687c71fb811004cef4262801758acd64ef757953c3a986a81f0c308c7770253ebda48eb499f74e71ccfb06128ee2d384326c5a4fb2d873ef540d7ad47f89eb7029eeeb999ac2cd0f1ed673add9cbaea7e9608051396df70a2822a91d451c00114e35a4d28aa012f08cc3999655798aab666df0c4efa62ad99219b7470761c2a14ffe2b70b54a896367d3b43cc85b1ec4028f0bbf55bb960223d334aa266afd25377cf3e035ec8960874f7aef1c59e683434eceab00d6ef7",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-7.0-2021-07-21/cv-corpus-7.0-2021-07-21-ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055429Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=534c55a398137bcbbcabb1c993cd7d3968c7c0eca9ec6196d7c8f124a8f6dc3089d2bce43998cc01e53983c0f85e96ded11ebed577f3f019ce4540091762086b6dfe78a15404acf8e16ec195e4f5bfed428d033d2c6375901b910d613ed0ed5d34c75fa6eb47572ae08c69b54c7d10e034ce3b279a43fc261f8d76d409043216013d300ff2824d7d1529e3fbfa4f7dd6e5f6813390d98bae58c3a282cc6c9db2183c462bd963f70ba41a0a786a324ef0a2472c81d3b4e283dc9055bd284fbf25030bdab5eaeb0c1745114ea50cf675b9550bb86f73e4213eeffaa0ae14a3b91e8b319f156471445ac3713138c66ad2366ca391795be0bad68a882b86aa260077",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-6.1-2020-12-11/ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055441Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=0d471295c03d3a98f3dec6b784cf76e820bc25dc673f07363607be24d174bc77ebfdfd4867e451504b8e8f355ff57238e9084ae1f9662bd70d3022cdb46d00bdfb22f0f3495dd9f294c03936b87318870a2496a83bccc8add8cb2fb78b5cdfc0730fe4494035c9700c77e4ba759163bccca5a4ba6f409d57196afb45f16bf37ff42e13700975950dbac14ee6f26fb4f04fe157d4679544d94d5d2411c67b68858ab61c175a034bb9ea494d50912b3d941cc572b73309b428998e79207bf0e09bacfaecbd3d11df32fea82f1376e1537dc0490b7d67591462a8da38a870634e6f99519b1e70bdc9d2fe6518ba60765a6ecffabdef0b284c9a95c24adfe390d67b",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-5.1-2020-06-22/ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055453Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=0787e00f7a78491a14c23459905498bdb0cf228aef12d90405fef089eafe66850826121eeeadb651c047e227dd769c66c0d337ad668a38bcd5a822ca545f1100be6767deae636bbf4970bf97824f7d5bb99a6de1c7c277cb1a50f7c77a9e024279c02ef0e8019f6ae71c2134215d71609db68ab417a9eb9c1c27c75cd10ae9ab7d2a268b86cb77ecf208ec8dade26d6ad7855ac47ee8c28eb8de1e53a999c889d8f0e69eca3e8b1458cfa31e35a689d4de4361f40d6314a29c5ec51cb0d18b7765f2e6dc7ac58024c6b1106af53879004688d1ced895a76ebc1ff5b74c3c44dd36e26a262265dc6776dce988d85343807842a52f0fadf27267801861e4e2e26b",
  "https://storage.googleapis.com/common-voice-prod-prod-datasets/cv-corpus-4-2019-12-10/ta.tar.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gke-prod%40moz-fx-common-voice-prod.iam.gserviceaccount.com%2F20240717%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240717T055504Z&X-Goog-Expires=43200&X-Goog-SignedHeaders=host&X-Goog-Signature=36e119a7abe2ff86e6bfc3c7da8f91e8590b91ecab5601e335825560f7f6a3ea220db669c00dc46f308023b2585f02bef6850e5fd16dd8a83b1477fc8367fd7528ebe84fa33d432f9f6f3f2024f1033f722f9f5ab737e9cdf7975ac36dc6b1f95b791d10205c734dc1b07a2de9f81dc34c8e498b0b3fe2a01c8b2df348d4c894c4d5f0fe23ebfc9a18e3c0c6c49d814d6abeaa347d83a2ac01de48a8992936b3a63312775d7021965582308b46f1afbfc5df56091883aee11c24d596376b28b835b1b2467a3cd8997c3aabf84645d0fef628f62e8f4cdb02f6fbb309331b8fb4aec02a64a248135069cde3a0d221e770e443be6a72fa54e06632e59c6f1a0baa"
]

# AWS S3 Configuration
bucket_name = ''
aws_access_key_id = ''
aws_secret_access_key = ''
aws_region = ''

# Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)


# Traverse the array of URLs and upload each file to S3
for url in urls:
    upload_to_s3(url, bucket_name)
