use object_manager::ObjectManagerConf;

fn main() {

    let conf = ObjectManagerConf::default();

    println!("{}",conf.to_json());

    
}