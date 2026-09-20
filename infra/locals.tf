locals {
    file_names = {
        for count in range(1, 8) : format("%02d", count) => {
            volume = var.files.volume
            path   = replace(var.files.path, "$()", format("%02d", count))
        }
    }
}